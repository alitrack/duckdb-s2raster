//! DuckDB Raster Extension — S2 geography + ST_Transform + GeoTIFF
//! Ported from SedonaDB algorithms, pure Rust, zero system deps.

use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::arrow::record_batch_to_duckdb_data_chunk;
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab, WritableVector};
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::{duckdb_entrypoint_c_api, Connection, Result};
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// ═══════════════════════════════════════════════════════════════════════════
// S2 Geography — ported from SedonaDB S2 function catalog
// ═══════════════════════════════════════════════════════════════════════════

fn s2_cell_id_impl(lat: f64, lon: f64, level: i64) -> i64 {
    let ll = s2::latlng::LatLng::from_degrees(lat, lon);
    let point = s2::point::Point::from(ll);
    let cell = s2::cellid::CellID::from(point);
    cell.parent(level as u64).0 as i64
}

fn s2_contains_impl(cell_id: i64, lat: f64, lon: f64) -> bool {
    let cell = s2::cellid::CellID(cell_id as u64);
    let c: s2::cell::Cell = cell.into();
    let ll = s2::latlng::LatLng::from_degrees(lat, lon);
    let point = s2::point::Point::from(ll);
    c.contains_point(&point)
}

fn s2_distance_meters_impl(cell1: i64, cell2: i64) -> f64 {
    let c1: s2::cell::Cell = s2::cellid::CellID(cell1 as u64).into();
    let c2: s2::cell::Cell = s2::cellid::CellID(cell2 as u64).into();
    let p1 = c1.center();
    let p2 = c2.center();
    // Use angle between vectors
    let dot = p1.0.dot(&p2.0);
    let angle = dot.min(1.0).max(-1.0).acos();
    angle * 6371009.0 // Earth mean radius in meters
}

fn s2_area_m2_impl(cell_id: i64, level: i32) -> f64 {
    let cell_id = s2::cellid::CellID(cell_id as u64).parent(level as u64);
    let c: s2::cell::Cell = cell_id.into();
    c.approx_area() * 6371009.0_f64.powi(2)
}

fn s2_parent_impl(cell_id: i64, level: i64) -> i64 {
    s2::cellid::CellID(cell_id as u64).parent(level as u64).0 as i64
}

// ═══════════════════════════════════════════════════════════════════════════
// ST_Transform — PROJ coordinate transformation
// ═══════════════════════════════════════════════════════════════════════════

fn st_transform_coords_impl(x: f64, y: f64, from_crs: &str, to_crs: &str) -> Result<String, String> {
    use proj::Proj;
    let proj = Proj::new_known_crs(from_crs, to_crs, None)
        .map_err(|e| format!("PROJ error: {}", e))?;
    proj.convert((x, y))
        .map(|(nx, ny)| format!("POINT({} {})", nx, ny))
        .map_err(|e| format!("Transform error: {}", e))
}

fn st_transform_impl(wkt_str: &str, from_crs: &str, to_crs: &str) -> Result<String, String> {
    use geo::geometry::Geometry;
    use geo::MapCoords;
    use proj::Proj;

    let geom = Geometry::try_from_wkt_str(wkt_str)
        .map_err(|e| format!("WKT parse error: {}", e))?;
    let proj = Proj::new_known_crs(from_crs, to_crs, None)
        .map_err(|e| format!("PROJ error: {}", e))?;

    let transformed = geom
        .map_coords(|c| proj.convert(c).unwrap_or(c))
        .map_err(|_| "Transform error")?;

    Ok(transformed.to_wkt())
}

// ═══════════════════════════════════════════════════════════════════════════
// GeoTIFF Raster — pure Rust, no GDAL
// ═══════════════════════════════════════════════════════════════════════════

mod raster {
    use std::collections::HashMap;
    use std::io::BufReader;
    use std::fs::File;
    use tiff::decoder::{Decoder, DecodingResult};

    pub struct RasterInfo {
        pub width: u32,
        pub height: u32,
        pub bands: u32,
        pub crs: String,
        pub pixel_scale: Option<(f64, f64)>,
        pub tie_point: Option<(f64, f64)>,
        pub no_data: Option<f64>,
    }

    pub fn read_metadata(path: &str) -> Result<RasterInfo, String> {
        let file = File::open(path).map_err(|e| format!("Cannot open: {}", e))?;
        let reader = BufReader::new(file);
        let mut decoder = Decoder::new(reader).map_err(|e| format!("TIFF decode: {}", e))?;

        let (width, height) = decoder.dimensions().map_err(|e| format!("{}", e))?;

        // Count bands
        let bands = decoder.find_tag_u32(tiff::tags::Tag::SamplesPerPixel)
            .ok()
            .map(|v| v as u32)
            .unwrap_or(1);

        // Parse GeoTIFF keys manually from tag 34735 (GeoKeyDirectoryTag)
        let mut geo_keys = HashMap::new();
        if let Ok(data) = decoder.find_tag_uint_vec(tiff::tags::Tag::GeoKeyDirectoryTag) {
            if data.len() >= 8 {
                let num_keys = (data[3] & 0xFFFF) as usize;
                for i in 0..num_keys {
                    let off = 4 + i * 4;
                    if off + 4 <= data.len() {
                        let key_id = (data[off] & 0xFFFF) as u16;
                        let count = (data[off + 2] & 0xFFFF) as u16;
                        let val = (data[off + 3] & 0xFFFF) as u16;
                        geo_keys.insert(key_id, (count, val));
                    }
                }
            }
        }

        // Determine CRS
        let epsg = geo_keys.get(&3072).or_else(|| geo_keys.get(&2048)).map(|(_, v)| *v as u32);
        let crs = match epsg {
            Some(0) | None => "Unknown".to_string(),
            Some(code) => format!("EPSG:{}", code),
        };

        // ModelPixelScaleTag (33550) — 3 doubles
        let pixel_scale = decoder.find_tag_f64_vec(tiff::tags::Tag::ModelPixelScaleTag)
            .ok()
            .and_then(|v| if v.len() >= 2 { Some((v[0], v[1])) } else { None });

        // ModelTiepointTag (33922) — 6+ doubles (I,J,K,X,Y,Z)
        let tie_point = decoder.find_tag_f64_vec(tiff::tags::Tag::ModelTiepointTag)
            .ok()
            .and_then(|v| if v.len() >= 6 { Some((v[3], v[4])) } else { None });

        let no_data = decoder.find_tag_f64_vec(tiff::tags::Tag::GDALNoData)
            .ok()
            .and_then(|v| v.first().copied());

        Ok(RasterInfo { width, height, bands, crs, pixel_scale, tie_point, no_data })
    }

    pub fn read_pixel(path: &str, band: u32, col: u32, row: u32) -> Result<f64, String> {
        let file = File::open(path).map_err(|e| format!("Cannot open: {}", e))?;
        let reader = BufReader::new(file);
        let mut decoder = Decoder::new(reader).map_err(|e| format!("TIFF decode: {}", e))?;

        let (width, height) = decoder.dimensions().map_err(|e| format!("{}", e))?;
        if col >= width || row >= height {
            return Err(format!("Pixel out of bounds"));
        }

        let img = decoder.read_image().map_err(|e| format!("Read error: {}", e))?;
        let idx = (row as usize * width as usize + col as usize)
            + (band.saturating_sub(1) as usize) * (width as usize * height as usize);

        match img {
            DecodingResult::F64(data) => Ok(*data.get(idx).unwrap_or(&f64::NAN)),
            DecodingResult::F32(data) => Ok(*data.get(idx).unwrap_or(&f32::NAN) as f64),
            DecodingResult::U16(data) => Ok(*data.get(idx).unwrap_or(&0) as f64),
            DecodingResult::U8(data) => Ok(*data.get(idx).unwrap_or(&0) as f64),
            _ => Ok(f64::NAN),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Scalar Functions (VScalar trait)
// ═══════════════════════════════════════════════════════════════════════════

use duckdb::vscalar::arrow::{ArrowScalarParams, VArrowScalar};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

macro_rules! simple_scalar {
    ($name:ident, $params:ty, $ret:ty, |$($arg:ident),*| $body:expr) => {
        struct $name;
        impl VArrowScalar for $name {
            fn invoke_arrow(&self, _params: &ArrowScalarParams) -> Result<arrow::array::ArrayRef, Box<dyn Error>> {
                let args = _params.args;
                $(
                    let $arg = args.get(0).unwrap().clone();
                    let args = &args[1..];
                )*
                let _ = args;
                Ok(Arc::new($body))
            }
            fn signatures() -> Vec<duckdb::vscalar::ScalarFunctionSignature> {
                vec![ScalarFunctionSignature::ExactArgs(stringify!($name).to_string(), vec![$params], $ret)]
            }
        }
    };
}

// Simple scalar functions using closures registered directly
fn register_simple(con: &Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<S2CellId>("s2_cell_id")?;
    con.register_scalar_function::<S2Contains>("s2_contains")?;
    con.register_scalar_function::<S2DistanceMeters>("s2_distance_meters")?;
    con.register_scalar_function::<S2AreaM2>("s2_area_m2")?;
    con.register_scalar_function::<S2Parent>("s2_parent")?;
    con.register_scalar_function::<StTransformCoords>("st_transform_coords")?;
    con.register_scalar_function::<StTransform>("st_transform")?;
    con.register_scalar_function::<RsValue>("rs_value")?;
    Ok(())
}

// Each scalar function is a struct implementing VArrowScalar

struct S2CellId;
impl VArrowScalar for S2CellId {
    fn invoke_arrow(&self, p: &ArrowScalarParams) -> Result<arrow::array::ArrayRef, Box<dyn Error>> {
        let lat = p.get_input_array::<Float64Array>(0)?;
        let lon = p.get_input_array::<Float64Array>(1)?;
        let lvl = p.get_input_array::<Int64Array>(2)?;
        let mut out = Int64Array::builder(lat.len());
        for i in 0..lat.len() {
            out.append_value(s2_cell_id_impl(lat.value(i), lon.value(i), lvl.value(i)));
        }
        Ok(Arc::new(out.finish()))
    }
    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::ExactArgs(
            "s2_cell_id".into(),
            vec![DataType::Float64, DataType::Float64, DataType::Int64],
            DataType::Int64,
        )]
    }
}

struct S2Contains;
impl VArrowScalar for S2Contains {
    fn invoke_arrow(&self, p: &ArrowScalarParams) -> Result<arrow::array::ArrayRef, Box<dyn Error>> {
        let cell = p.get_input_array::<Int64Array>(0)?;
        let lat = p.get_input_array::<Float64Array>(1)?;
        let lon = p.get_input_array::<Float64Array>(2)?;
        let mut out = arrow::array::BooleanArray::builder(lat.len());
        for i in 0..lat.len() {
            out.append_value(s2_contains_impl(cell.value(i), lat.value(i), lon.value(i)));
        }
        Ok(Arc::new(out.finish()))
    }
    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::ExactArgs(
            "s2_contains".into(),
            vec![DataType::Int64, DataType::Float64, DataType::Float64],
            DataType::Boolean,
        )]
    }
}

struct S2DistanceMeters;
impl VArrowScalar for S2DistanceMeters {
    fn invoke_arrow(&self, p: &ArrowScalarParams) -> Result<arrow::array::ArrayRef, Box<dyn Error>> {
        let c1 = p.get_input_array::<Int64Array>(0)?;
        let c2 = p.get_input_array::<Int64Array>(1)?;
        let mut out = Float64Array::builder(c1.len());
        for i in 0..c1.len() {
            out.append_value(s2_distance_meters_impl(c1.value(i), c2.value(i)));
        }
        Ok(Arc::new(out.finish()))
    }
    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::ExactArgs(
            "s2_distance_meters".into(),
            vec![DataType::Int64, DataType::Int64],
            DataType::Float64,
        )]
    }
}

struct S2AreaM2;
impl VArrowScalar for S2AreaM2 {
    fn invoke_arrow(&self, p: &ArrowScalarParams) -> Result<arrow::array::ArrayRef, Box<dyn Error>> {
        let cell = p.get_input_array::<Int64Array>(0)?;
        let lvl = p.get_input_array::<Int32Array>(1)?;
        let mut out = Float64Array::builder(cell.len());
        for i in 0..cell.len() {
            out.append_value(s2_area_m2_impl(cell.value(i), lvl.value(i)));
        }
        Ok(Arc::new(out.finish()))
    }
    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::ExactArgs(
            "s2_area_m2".into(),
            vec![DataType::Int64, DataType::Int32],
            DataType::Float64,
        )]
    }
}

struct S2Parent;
impl VArrowScalar for S2Parent {
    fn invoke_arrow(&self, p: &ArrowScalarParams) -> Result<arrow::array::ArrayRef, Box<dyn Error>> {
        let cell = p.get_input_array::<Int64Array>(0)?;
        let lvl = p.get_input_array::<Int64Array>(1)?;
        let mut out = Int64Array::builder(cell.len());
        for i in 0..cell.len() {
            out.append_value(s2_parent_impl(cell.value(i), lvl.value(i)));
        }
        Ok(Arc::new(out.finish()))
    }
    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::ExactArgs(
            "s2_parent".into(),
            vec![DataType::Int64, DataType::Int64],
            DataType::Int64,
        )]
    }
}

struct StTransformCoords;
impl VArrowScalar for StTransformCoords {
    fn invoke_arrow(&self, p: &ArrowScalarParams) -> Result<arrow::array::ArrayRef, Box<dyn Error>> {
        let x = p.get_input_array::<Float64Array>(0)?;
        let y = p.get_input_array::<Float64Array>(1)?;
        let from = p.get_input_array::<StringArray>(2)?;
        let to = p.get_input_array::<StringArray>(3)?;
        let mut out = StringArray::builder(x.len());
        for i in 0..x.len() {
            let r = st_transform_coords_impl(x.value(i), y.value(i), from.value(i), to.value(i))
                .unwrap_or_else(|e| format!("ERROR: {}", e));
            out.append_value(&r);
        }
        Ok(Arc::new(out.finish()))
    }
    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::ExactArgs(
            "st_transform_coords".into(),
            vec![DataType::Float64, DataType::Float64, DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
        )]
    }
}

struct StTransform;
impl VArrowScalar for StTransform {
    fn invoke_arrow(&self, p: &ArrowScalarParams) -> Result<arrow::array::ArrayRef, Box<dyn Error>> {
        let wkt = p.get_input_array::<StringArray>(0)?;
        let from = p.get_input_array::<StringArray>(1)?;
        let to = p.get_input_array::<StringArray>(2)?;
        let mut out = StringArray::builder(wkt.len());
        for i in 0..wkt.len() {
            let r = st_transform_impl(wkt.value(i), from.value(i), to.value(i))
                .unwrap_or_else(|e| format!("ERROR: {}", e));
            out.append_value(&r);
        }
        Ok(Arc::new(out.finish()))
    }
    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::ExactArgs(
            "st_transform".into(),
            vec![DataType::Utf8, DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
        )]
    }
}

struct RsValue;
impl VArrowScalar for RsValue {
    fn invoke_arrow(&self, p: &ArrowScalarParams) -> Result<arrow::array::ArrayRef, Box<dyn Error>> {
        let path = p.get_input_array::<StringArray>(0)?;
        let band = p.get_input_array::<Int32Array>(1)?;
        let col = p.get_input_array::<Int32Array>(2)?;
        let row = p.get_input_array::<Int32Array>(3)?;
        let mut out = Float64Array::builder(path.len());
        for i in 0..path.len() {
            let v = raster::read_pixel(path.value(i), band.value(i).max(1) as u32, col.value(i).max(0) as u32, row.value(i).max(0) as u32)
                .unwrap_or(f64::NAN);
            out.append_value(v);
        }
        Ok(Arc::new(out.finish()))
    }
    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::ExactArgs(
            "rs_value".into(),
            vec![DataType::Utf8, DataType::Int32, DataType::Int32, DataType::Int32],
            DataType::Float64,
        )]
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Table Functions: rs_metadata(path) → table
// ═══════════════════════════════════════════════════════════════════════════

pub struct RsMetadataVTab;
#[repr(C)]
pub struct RsMetadataInit { done: AtomicBool }
#[repr(C)]
pub struct RsMetadataBind {
    results: Vec<RasterRow>,
    schema: Arc<Schema>,
}
struct RasterRow {
    path: String, width: i32, height: i32, bands: i32, crs: String,
    scale_x: f64, scale_y: f64, tie_x: f64, tie_y: f64, nodata: f64,
}

impl VTab for RsMetadataVTab {
    type BindData = RsMetadataBind;
    type InitData = RsMetadataInit;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("path", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("width", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("height", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("bands", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("crs", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("scale_x", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("scale_y", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("tie_x", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("tie_y", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("nodata", LogicalTypeHandle::from(LogicalTypeId::Double));

        let path = bind.get_parameter::<String>(0)?;
        let info = raster::read_metadata(&path)?;

        Ok(RsMetadataBind {
            results: vec![RasterRow {
                path, width: info.width as i32, height: info.height as i32,
                bands: info.bands as i32, crs: info.crs,
                scale_x: info.pixel_scale.map(|s| s.0).unwrap_or(f64::NAN),
                scale_y: info.pixel_scale.map(|s| s.1).unwrap_or(f64::NAN),
                tie_x: info.tie_point.map(|t| t.0).unwrap_or(f64::NAN),
                tie_y: info.tie_point.map(|t| t.1).unwrap_or(f64::NAN),
                nodata: info.no_data.unwrap_or(f64::NAN),
            }],
            schema: Arc::new(Schema::new(vec![
                Field::new("path", DataType::Utf8, false),
                Field::new("width", DataType::Int32, false),
                Field::new("height", DataType::Int32, false),
                Field::new("bands", DataType::Int32, false),
                Field::new("crs", DataType::Utf8, false),
                Field::new("scale_x", DataType::Float64, true),
                Field::new("scale_y", DataType::Float64, true),
                Field::new("tie_x", DataType::Float64, true),
                Field::new("tie_y", DataType::Float64, true),
                Field::new("nodata", DataType::Float64, true),
            ])),
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(RsMetadataInit { done: AtomicBool::new(false) })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        let bind_data = func.get_bind_data();
        let r = &bind_data.results[0];
        use arrow::array::{Float64Array, Int32Array, StringArray};
        let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(StringArray::from(vec![r.path.as_str()])),
            Arc::new(Int32Array::from(vec![r.width])),
            Arc::new(Int32Array::from(vec![r.height])),
            Arc::new(Int32Array::from(vec![r.bands])),
            Arc::new(StringArray::from(vec![r.crs.as_str()])),
            Arc::new(Float64Array::from(vec![r.scale_x])),
            Arc::new(Float64Array::from(vec![r.scale_y])),
            Arc::new(Float64Array::from(vec![r.tie_x])),
            Arc::new(Float64Array::from(vec![r.tie_y])),
            Arc::new(Float64Array::from(vec![r.nodata])),
        ];
        let batch = arrow::array::RecordBatch::try_new(bind_data.schema.clone(), arrays)?;
        record_batch_to_duckdb_data_chunk(&batch, output)?;
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Table Functions: s2_covering(wkt, min_level, max_level) → table(cell_id)
// ═══════════════════════════════════════════════════════════════════════════

pub struct S2CoveringVTab;
#[repr(C)]
pub struct S2CoveringInit { done: AtomicBool }
#[repr(C)]
pub struct S2CoveringBind {
    cell_ids: Vec<i64>,
    schema: Arc<Schema>,
}

impl VTab for S2CoveringVTab {
    type BindData = S2CoveringBind;
    type InitData = S2CoveringInit;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("cell_id", LogicalTypeHandle::from(LogicalTypeId::Bigint));

        let wkt_str = bind.get_parameter::<String>(0)?;
        let min_level = bind.get_parameter::<i32>(1)?;
        let max_level = bind.get_parameter::<i32>(2)?;

        use geo::geometry::Geometry;
        use geo::BoundingRect;

        let geom = Geometry::try_from_wkt_str(&wkt_str)
            .map_err(|e| format!("WKT parse error: {}", e))?;
        let bbox = geom.bounding_rect().ok_or("No bounding box")?;

        let rect = s2::rect::Rect::from_degrees(
            bbox.min().y, bbox.min().x,
            bbox.max().y, bbox.max().x,
        );

        let coverer = s2::region::RegionCoverer {
            min_level: min_level as u8,
            max_level: max_level as u8,
            level_mod: 1,
            max_cells: 100,
        };
        let covering = coverer.covering(&rect);

        Ok(S2CoveringBind {
            cell_ids: covering.iter().map(|c| c.0 as i64).collect(),
            schema: Arc::new(Schema::new(vec![
                Field::new("cell_id", DataType::Int64, false),
            ])),
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(S2CoveringInit { done: AtomicBool::new(false) })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        let bind_data = func.get_bind_data();
        use arrow::array::Int64Array;
        let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(Int64Array::from(bind_data.cell_ids.clone())),
        ];
        let batch = arrow::array::RecordBatch::try_new(bind_data.schema.clone(), arrays)?;
        record_batch_to_duckdb_data_chunk(&batch, output)?;
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Integer),
            LogicalTypeHandle::from(LogicalTypeId::Integer),
        ])
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Entrypoint
// ═══════════════════════════════════════════════════════════════════════════

#[duckdb_entrypoint_c_api(ext_name = "raster")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    register_simple(&con)?;
    con.register_table_function::<RsMetadataVTab>("rs_metadata")?;
    con.register_table_function::<S2CoveringVTab>("s2_covering")?;
    Ok(())
}

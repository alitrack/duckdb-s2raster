//! DuckDB Raster Extension — S2 geography + ST_Transform + GeoTIFF
//! Ported from SedonaDB algorithms, pure Rust, zero system deps.

use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vscalar::arrow::{ArrowFunctionSignature, VArrowScalar};
use duckdb::vtab::arrow::record_batch_to_duckdb_data_chunk;
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::{Connection, Result};
#[cfg(feature = "loadable-extension")]
use duckdb::duckdb_entrypoint_c_api;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// ─── S2 Geography ───────────────────────────────────────────────────────────

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
    let dot = p1.0.dot(&p2.0);
    let angle = dot.min(1.0).max(-1.0).acos();
    angle * 6_371_009.0
}

fn s2_area_m2_impl(cell_id: i64, level: i32) -> f64 {
    let cid = s2::cellid::CellID(cell_id as u64).parent(level as u64);
    let c: s2::cell::Cell = cid.into();
    c.approx_area() * 6_371_009.0_f64.powi(2)
}

fn s2_parent_impl(cell_id: i64, level: i64) -> i64 {
    s2::cellid::CellID(cell_id as u64).parent(level as u64).0 as i64
}

// ─── ST_Transform ───────────────────────────────────────────────────────────

fn st_transform_coords_impl(x: f64, y: f64, from_crs: &str, to_crs: &str) -> String {
    use proj::Proj;
    match Proj::new_known_crs(from_crs, to_crs, None) {
        Ok(p) => match p.convert((x, y)) {
            Ok((nx, ny)) => format!("POINT({} {})", nx, ny),
            Err(e) => format!("ERROR: {}", e),
        },
        Err(e) => format!("ERROR: {}", e),
    }
}

fn st_transform_impl(wkt_str: &str, from_crs: &str, to_crs: &str) -> String {
    use geo::MapCoords;
    use proj::Proj;
    use wkt::{ToWkt, TryFromWkt};
    let geom = match geo::geometry::Geometry::<f64>::try_from_wkt_str(wkt_str) {
        Ok(g) => g,
        Err(e) => return format!("ERROR: {}", e),
    };
    let proj = match Proj::new_known_crs(from_crs, to_crs, None) {
        Ok(p) => p,
        Err(e) => return format!("ERROR: {}", e),
    };
    let transformed = geom.map_coords(|c| proj.convert(c).unwrap_or(c));
    transformed.to_wkt().to_string()
}

// ─── GeoTIFF Raster ────────────────────────────────────────────────────────

mod raster {
    use std::fs::File;
    use std::io::BufReader;
    use tiff::decoder::{Decoder, DecodingResult};

    pub fn read_metadata(path: &str) -> Result<(u32, u32, u32, String), String> {
        let file = File::open(path).map_err(|e| format!("Cannot open: {}", e))?;
        let reader = BufReader::new(file);
        let mut decoder = Decoder::new(reader).map_err(|e| format!("TIFF: {}", e))?;
        let (w, h) = decoder.dimensions().map_err(|e| format!("{}", e))?;
        let bands = decoder
            .find_tag_unsigned::<u32>(tiff::tags::Tag::SamplesPerPixel)
            .ok()
            .flatten()
            .unwrap_or(1);
        let crs = "See GeoTIFF tags".to_string();
        Ok((w, h, bands, crs))
    }

    pub fn read_pixel(path: &str, band: u32, col: u32, row: u32) -> Result<f64, String> {
        let file = File::open(path).map_err(|e| format!("Cannot open: {}", e))?;
        let reader = BufReader::new(file);
        let mut decoder = Decoder::new(reader).map_err(|e| format!("TIFF: {}", e))?;
        let (width, height) = decoder.dimensions().map_err(|e| format!("{}", e))?;
        if col >= width || row >= height {
            return Err("Pixel out of bounds".into());
        }
        let img = decoder.read_image().map_err(|e| format!("Read: {}", e))?;
        let idx = (row as usize * width as usize + col as usize)
            + band.saturating_sub(1) as usize * (width as usize * height as usize);
        match img {
            DecodingResult::F64(data) => Ok(*data.get(idx).unwrap_or(&f64::NAN)),
            DecodingResult::F32(data) => Ok(*data.get(idx).unwrap_or(&0.0) as f64),
            DecodingResult::U16(data) => Ok(*data.get(idx).unwrap_or(&0) as f64),
            DecodingResult::U8(data) => Ok(*data.get(idx).unwrap_or(&0) as f64),
            _ => Ok(f64::NAN),
        }
    }
}

// ─── VArrowScalar implementations ──────────────────────────────────────────

pub struct S2CellId;
impl VArrowScalar for S2CellId {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let lat = input.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let lon = input.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let lvl = input.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let mut b = Int64Array::builder(lat.len());
        for i in 0..lat.len() { b.append_value(s2_cell_id_impl(lat.value(i), lon.value(i), lvl.value(i))); }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(vec![DataType::Float64, DataType::Float64, DataType::Int64], DataType::Int64)]
    }
}

pub struct S2Contains;
impl VArrowScalar for S2Contains {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let cell = input.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let lat = input.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let lon = input.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let mut b = BooleanArray::builder(lat.len());
        for i in 0..lat.len() { b.append_value(s2_contains_impl(cell.value(i), lat.value(i), lon.value(i))); }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(vec![DataType::Int64, DataType::Float64, DataType::Float64], DataType::Boolean)]
    }
}

pub struct S2Distance;
impl VArrowScalar for S2Distance {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let c1 = input.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let c2 = input.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let mut b = Float64Array::builder(c1.len());
        for i in 0..c1.len() { b.append_value(s2_distance_meters_impl(c1.value(i), c2.value(i))); }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(vec![DataType::Int64, DataType::Int64], DataType::Float64)]
    }
}

pub struct S2Area;
impl VArrowScalar for S2Area {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let cell = input.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let lvl = input.column(1).as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        let mut b = Float64Array::builder(cell.len());
        for i in 0..cell.len() { b.append_value(s2_area_m2_impl(cell.value(i), lvl.value(i))); }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(vec![DataType::Int64, DataType::Int32], DataType::Float64)]
    }
}

pub struct S2Parent;
impl VArrowScalar for S2Parent {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let cell = input.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let lvl = input.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let mut b = Int64Array::builder(cell.len());
        for i in 0..cell.len() { b.append_value(s2_parent_impl(cell.value(i), lvl.value(i))); }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(vec![DataType::Int64, DataType::Int64], DataType::Int64)]
    }
}

pub struct StTransformCoords;
impl VArrowScalar for StTransformCoords {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let x = input.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let y = input.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let from = input.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let to = input.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        let mut b = StringBuilder::with_capacity(x.len(), x.len() * 20);
        for i in 0..x.len() { b.append_value(st_transform_coords_impl(x.value(i), y.value(i), from.value(i), to.value(i))); }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(vec![DataType::Float64, DataType::Float64, DataType::Utf8, DataType::Utf8], DataType::Utf8)]
    }
}

pub struct StTransform;
impl VArrowScalar for StTransform {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let wkt = input.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let from = input.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let to = input.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let mut b = StringBuilder::with_capacity(wkt.len(), wkt.len() * 100);
        for i in 0..wkt.len() { b.append_value(st_transform_impl(wkt.value(i), from.value(i), to.value(i))); }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8], DataType::Utf8)]
    }
}

pub struct RsValue;
impl VArrowScalar for RsValue {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let band = input.column(1).as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        let col = input.column(2).as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        let row = input.column(3).as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        let mut b = Float64Array::builder(path.len());
        for i in 0..path.len() {
            b.append_value(raster::read_pixel(path.value(i), band.value(i).max(1) as u32, col.value(i).max(0) as u32, row.value(i).max(0) as u32).unwrap_or(f64::NAN));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(vec![DataType::Utf8, DataType::Int32, DataType::Int32, DataType::Int32], DataType::Float64)]
    }
}

// ─── Table Function: rs_metadata(path) → (path, width, height, bands, crs) ─

pub struct RsMetadataVTab;
#[repr(C)] pub struct RsMetaInit { done: AtomicBool }
#[repr(C)] pub struct RsMetaBind { row: Option<MetaRow>, schema: Arc<Schema> }
struct MetaRow { path: String, width: i32, height: i32, bands: i32, crs: String }

impl VTab for RsMetadataVTab {
    type BindData = RsMetaBind;
    type InitData = RsMetaInit;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("path", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("width", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("height", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("bands", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("crs", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        let path = bind.get_parameter(0).to_string();
        let (w, h, b, crs) = raster::read_metadata(&path)?;
        Ok(RsMetaBind {
            row: Some(MetaRow { path, width: w as i32, height: h as i32, bands: b as i32, crs }),
            schema: Arc::new(Schema::new(vec![
                Field::new("path", DataType::Utf8, false),
                Field::new("width", DataType::Int32, false),
                Field::new("height", DataType::Int32, false),
                Field::new("bands", DataType::Int32, false),
                Field::new("crs", DataType::Utf8, false),
            ])),
        })
    }
    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(RsMetaInit { done: AtomicBool::new(false) })
    }
    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        if func.get_init_data().done.swap(true, Ordering::Relaxed) { output.set_len(0); return Ok(()); }
        let bd = func.get_bind_data();
        let r = bd.row.as_ref().unwrap();
        use arrow::array::Int32Array;
        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(StringArray::from(vec![r.path.as_str()])),
            Arc::new(Int32Array::from(vec![r.width])),
            Arc::new(Int32Array::from(vec![r.height])),
            Arc::new(Int32Array::from(vec![r.bands])),
            Arc::new(StringArray::from(vec![r.crs.as_str()])),
        ];
        let batch = RecordBatch::try_new(bd.schema.clone(), arrays)?;
        record_batch_to_duckdb_data_chunk(&batch, output)?;
        Ok(())
    }
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// ─── Entrypoint ────────────────────────────────────────────────────────────

#[cfg(feature = "loadable-extension")]
#[duckdb_entrypoint_c_api(ext_name = "raster")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<S2CellId>("s2_cell_id")?;
    con.register_scalar_function::<S2Contains>("s2_contains")?;
    con.register_scalar_function::<S2Distance>("s2_distance_meters")?;
    con.register_scalar_function::<S2Area>("s2_area_m2")?;
    con.register_scalar_function::<S2Parent>("s2_parent")?;
    con.register_scalar_function::<StTransformCoords>("st_transform_coords")?;
    con.register_scalar_function::<StTransform>("st_transform")?;
    con.register_scalar_function::<RsValue>("rs_value")?;
    con.register_table_function::<RsMetadataVTab>("rs_metadata")?;
    Ok(())
}

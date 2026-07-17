//! DuckDB Raster Extension
//!
//! Ports SedonaDB's spatial algorithms into a DuckDB loadable extension:
//! - S2 Geography: cell_id, contains, distance, area, covering
//! - ST_Transform: coordinate/projection transformation via PROJ
//! - Raster: GeoTIFF metadata & pixel access (pure Rust, no GDAL)

use arrow::array::{Float64Array, Int64Array, StringArray, RecordBatch, UInt64Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::arrow::record_batch_to_duckdb_data_chunk;
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::{duckdb_entrypoint_c_api, scalar_func, Connection, Result};
use std::error::Error;
use std::sync::Arc;

// ─── S2 Geography ───────────────────────────────────────────────────────────

fn s2_cell_id(lat: f64, lon: f64, level: i64) -> i64 {
    let point = s2::point::Point::from(s2::latlng::LatLng::from_degrees(lat, lon).unwrap());
    let cell = s2::cellid::CellID::from(point);
    cell.parent(level as u64).0 as i64
}

fn s2_contains(cell_id: i64, lat: f64, lon: f64) -> bool {
    let cell = s2::cellid::CellID(cell_id as u64);
    let point = s2::point::Point::from(s2::latlng::LatLng::from_degrees(lat, lon).unwrap());
    cell.contains(&point)
}

fn s2_distance_meters(cell1: i64, cell2: i64) -> f64 {
    let c1 = s2::cellid::CellID(cell1 as u64);
    let c2 = s2::cellid::CellID(cell2 as u64);
    let p1 = s2::point::Point::from(c1.center());
    let p2 = s2::point::Point::from(c2.center());
    p1.angle(&p2).rad() * 6371009.0 // Earth mean radius in meters
}

fn s2_area_m2(cell_id: i64, level: i32) -> f64 {
    let cell = s2::cellid::CellID(cell_id as u64).parent(level as u64);
    cell.approx_area() * 6371009.0_f64.powi(2) // convert steradians to m²
}

fn s2_parent(cell_id: i64, level: i64) -> i64 {
    s2::cellid::CellID(cell_id as u64).parent(level as u64).0 as i64
}

// ─── ST_Transform ───────────────────────────────────────────────────────────

fn st_transform_coords(x: f64, y: f64, from_crs: &str, to_crs: &str) -> Result<(f64, f64), String> {
    use proj::Proj;
    let proj = Proj::new_known_crs(from_crs, to_crs, None)
        .map_err(|e| format!("PROJ error: {}", e))?;
    proj.convert((x, y))
        .map_err(|e| format!("Transform error: {}", e))
}

fn st_transform_geom(wkt_str: &str, from_crs: &str, to_crs: &str) -> Result<String, String> {
    use geo::geometry::Geometry;
    use proj::Proj;
    use wkt::TryFromWkt;

    let geom = Geometry::try_from_wkt_str(wkt_str)
        .map_err(|e| format!("WKT parse error: {}", e))?;
    let proj = Proj::new_known_crs(from_crs, to_crs, None)
        .map_err(|e| format!("PROJ error: {}", e))?;

    let transformed = geom
        .map_coords(|c| proj.convert(c).unwrap_or(c))
        .map_err(|e| format!("Transform error: {}", e))?;

    Ok(transformed.to_wkt())
}

// ─── GeoTIFF Raster ────────────────────────────────────────────────────────

mod raster {
    use std::collections::HashMap;
    use std::io::BufReader;
    use tiff::decoder::{Decoder, DecodingResult};

    /// Parsed GeoTIFF metadata
    pub struct RasterInfo {
        pub width: u32,
        pub height: u32,
        pub bands: u32,
        pub crs: String,
        pub pixel_scale: Option<(f64, f64, f64)>, // (x, y, z)
        pub tie_point: Option<(f64, f64, f64, f64, f64, f64)>, // (I,J,K,X,Y,Z)
        pub no_data: Option<f64>,
    }

    /// Read GeoTIFF metadata from file
    pub fn read_metadata(path: &str) -> Result<RasterInfo, String> {
        let file = std::fs::File::open(path).map_err(|e| format!("Cannot open: {}", e))?;
        let reader = BufReader::new(file);
        let mut decoder = Decoder::new(reader).map_err(|e| format!("TIFF decode error: {}", e))?;

        let dimensions = decoder.dimensions().map_err(|e| format!("{}", e))?;
        let width = dimensions.0;
        let height = dimensions.1;

        // Count bands from IFD tags
        let bands = match decoder.get_tag_u16(tiff::tags::Tag::SamplesPerPixel) {
            Ok(n) => n as u32,
            Err(_) => 1,
        };

        // Extract GeoTIFF tags
        let mut geo_keys = HashMap::new();
        if let Ok(data) = decoder.get_tag_bytes(tiff::tags::Tag::GeoKeyDirectoryTag) {
            // GeoKeyDirectory: {KeyDirVersion, KeyRevision, MinorRevision, NumKeys}
            // then {KeyID, TIFFTagLocation, Count, ValueOffset} * NumKeys
            if data.len() >= 8 {
                let num_keys = u16::from_le_bytes([data[6], data[7]]) as usize;
                for i in 0..num_keys {
                    let off = 8 + i * 8;
                    if off + 8 <= data.len() {
                        let key_id = u16::from_le_bytes([data[off], data[off+1]]);
                        let tag_loc = u16::from_le_bytes([data[off+2], data[off+3]]);
                        let count = u16::from_le_bytes([data[off+4], data[off+5]]);
                        let val_off = u16::from_le_bytes([data[off+6], data[off+7]]);
                        geo_keys.insert(key_id, (tag_loc, count, val_off));
                    }
                }
            }
        }

        // Determine CRS from GeoTIFF keys
        // Key 3072 = ProjectedCRSGeoKey, Key 2048 = GeographicTypeGeoKey
        let crs = if geo_keys.contains_key(&3072) || geo_keys.contains_key(&2048) {
            let epsg_code = geo_keys.get(&3072)
                .or_else(|| geo_keys.get(&2048))
                .map(|(_, _, v)| *v as u32)
                .unwrap_or(0);
            if epsg_code > 0 {
                format!("EPSG:{}", epsg_code)
            } else {
                "Unknown".to_string()
            }
        } else {
            "Unknown (no GeoTIFF keys)".to_string()
        };

        // ModelPixelScaleTag (33550): 3 doubles (ScaleX, ScaleY, ScaleZ)
        let pixel_scale = match decoder.get_tag_f64(tiff::tags::Tag::ModelPixelScaleTag) {
            Ok(v) if v.len() >= 2 => Some((v[0], v[1], if v.len() > 2 { v[2] } else { 0.0 })),
            _ => None,
        };

        // ModelTiepointTag (33922): 6 doubles (I,J,K,X,Y,Z)
        let tie_point = match decoder.get_tag_f64(tiff::tags::Tag::ModelTiepointTag) {
            Ok(v) if v.len() >= 6 => {
                Some((v[0], v[1], v[2], v[3], v[4], v[5]))
            }
            _ => None,
        };

        let no_data = match decoder.get_tag_f64(tiff::tags::Tag::GDALNoData) {
            Ok(v) if !v.is_empty() => Some(v[0]),
            _ => None,
        };

        Ok(RasterInfo { width, height, bands, crs, pixel_scale, tie_point, no_data })
    }

    /// Read a single pixel value from a GeoTIFF
    pub fn read_pixel(path: &str, band: u32, col: u32, row: u32) -> Result<f64, String> {
        let file = std::fs::File::open(path).map_err(|e| format!("Cannot open: {}", e))?;
        let reader = BufReader::new(file);
        let mut decoder = Decoder::new(reader).map_err(|e| format!("TIFF decode error: {}", e))?;

        let dimensions = decoder.dimensions().map_err(|e| format!("{}", e))?;
        if col >= dimensions.0 || row >= dimensions.1 {
            return Err(format!("Pixel ({},{}) out of bounds ({}x{})", col, row, dimensions.0, dimensions.1));
        }

        let img_result = decoder.read_image().map_err(|e| format!("Read error: {}", e))?;

        match img_result {
            DecodingResult::F64(data) => {
                let idx = ((row as usize * dimensions.0 as usize + col as usize) + (band as usize - 1) * (dimensions.0 * dimensions.1) as usize);
                Ok(*data.get(idx).unwrap_or(&f64::NAN))
            }
            DecodingResult::F32(data) => {
                let idx = (row as usize * dimensions.0 as usize + col as usize) + (band as usize - 1) * (dimensions.0 * dimensions.1) as usize;
                Ok(*data.get(idx).unwrap_or(&f32::NAN) as f64)
            }
            DecodingResult::U16(data) => {
                let idx = (row as usize * dimensions.0 as usize + col as usize) + (band as usize - 1) * (dimensions.0 * dimensions.1) as usize;
                Ok(*data.get(idx).unwrap_or(&0) as f64)
            }
            DecodingResult::U8(data) => {
                let idx = (row as usize * dimensions.0 as usize + col as usize) + (band as usize - 1) * (dimensions.0 * dimensions.1) as usize;
                Ok(*data.get(idx).unwrap_or(&0) as f64)
            }
            _ => Err("Unsupported pixel type".to_string()),
        }
    }
}

// ─── Scalar Functions ───────────────────────────────────────────────────────

fn register_scalar_functions(con: &Connection) -> Result<(), Box<dyn Error>> {
    // S2 cell_id(lat, lon, level) → cell_id
    con.register_scalar_function::<(f64, f64, i64), i64, _>("s2_cell_id", s2_cell_id)?;

    // S2 contains(cell_id, lat, lon) → bool
    con.register_scalar_function::<(i64, f64, f64), bool, _>("s2_contains", s2_contains)?;

    // S2 distance_meters(cell1, cell2) → distance
    con.register_scalar_function::<(i64, i64), f64, _>("s2_distance_meters", s2_distance_meters)?;

    // S2 area_m2(cell_id, level) → area
    con.register_scalar_function::<(i64, i32), f64, _>("s2_area_m2", s2_area_m2)?;

    // S2 parent(cell_id, level) → parent_cell_id
    con.register_scalar_function::<(i64, i64), i64, _>("s2_parent", s2_parent)?;

    // ST_Transform coords(x, y, from_crs, to_crs) → (x, y) as struct
    // Simplified: return WKT point
    con.register_scalar_function_ex::<(f64, f64, String, String), String, _>(
        "st_transform_coords", |x: f64, y: f64, from: String, to: String| -> String {
            st_transform_coords(x, y, &from, &to)
                .map(|(nx, ny)| format!("POINT({} {})", nx, ny))
                .unwrap_or_else(|e| format!("ERROR: {}", e))
        })?;

    // ST_Transform geometry(wkt, from_crs, to_crs) → wkt
    con.register_scalar_function_ex::<(String, String, String), String, _>(
        "st_transform", |wkt_str: String, from: String, to: String| -> String {
            st_transform_geom(&wkt_str, &from, &to)
                .unwrap_or_else(|e| format!("ERROR: {}", e))
        })?;

    // RS_Value(path, band, col, row) → pixel_value
    con.register_scalar_function_ex::<(String, i32, i32, i32), f64, _>(
        "rs_value", |path: String, band: i32, col: i32, row: i32| -> f64 {
            raster::read_pixel(&path, band.max(1) as u32, col.max(0) as u32, row.max(0) as u32)
                .unwrap_or(f64::NAN)
        })?;

    Ok(())
}

// ─── Table Functions ───────────────────────────────────────────────────────

/// rs_metadata(path) → table(width, height, bands, crs, scale_x, scale_y, tie_x, tie_y, nodata)
pub struct RsMetadataVTab;
#[repr(C)]
pub struct RsMetadataInit { done: std::sync::atomic::AtomicBool }
#[repr(C)]
pub struct RsMetadataBind {
    results: Vec<(String, RasterMetaRow)>,
    schema: Arc<Schema>,
}

struct RasterMetaRow {
    width: i32, height: i32, bands: i32, crs: String,
    scale_x: f64, scale_y: f64, tie_x: f64, tie_y: f64, nodata: f64,
}

impl VTab for RsMetadataVTab {
    type BindData = RsMetadataBind;
    type InitData = RsMetadataInit;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("path",       LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("width",      LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("height",     LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("bands",      LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("crs",        LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("scale_x",    LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("scale_y",    LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("tie_x",      LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("tie_y",      LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("nodata",     LogicalTypeHandle::from(LogicalTypeId::Double));

        let path = bind.get_parameter::<String>(0)?;
        let info = raster::read_metadata(&path)?;

        let schema = Arc::new(Schema::new(vec![
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
        ]));

        let row = RasterMetaRow {
            width: info.width as i32, height: info.height as i32, bands: info.bands as i32,
            crs: info.crs,
            scale_x: info.pixel_scale.map(|s| s.0).unwrap_or(f64::NAN),
            scale_y: info.pixel_scale.map(|s| s.1).unwrap_or(f64::NAN),
            tie_x: info.tie_point.map(|t| t.3).unwrap_or(f64::NAN),
            tie_y: info.tie_point.map(|t| t.4).unwrap_or(f64::NAN),
            nodata: info.no_data.unwrap_or(f64::NAN),
        };

        Ok(RsMetadataBind {
            results: vec![(path, row)],
            schema,
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(RsMetadataInit { done: false.into() })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        if func.init_data.done.swap(true, std::sync::atomic::Ordering::SeqCst) {
            output.set_len(0);
            return Ok(());
        }

        let batch = {
            let bind = &func.bind_data;
            let r = &bind.results[0];
            let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
                Arc::new(StringArray::from(vec![r.0.as_str()])),
                Arc::new(Int32Array::from(vec![r.1.width])),
                Arc::new(Int32Array::from(vec![r.1.height])),
                Arc::new(Int32Array::from(vec![r.1.bands])),
                Arc::new(StringArray::from(vec![r.1.crs.as_str()])),
                Arc::new(Float64Array::from(vec![r.1.scale_x])),
                Arc::new(Float64Array::from(vec![r.1.scale_y])),
                Arc::new(Float64Array::from(vec![r.1.tie_x])),
                Arc::new(Float64Array::from(vec![r.1.tie_y])),
                Arc::new(Float64Array::from(vec![r.1.nodata])),
            ];
            RecordBatch::try_new(bind.schema.clone(), arrays)?
        };

        record_batch_to_duckdb_data_chunk(&batch, output)?;
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

/// s2_covering(wkt, min_level, max_level) → table(cell_id)
pub struct S2CoveringVTab;
#[repr(C)]
pub struct S2CoveringInit { done: std::sync::atomic::AtomicBool }
#[repr(C)]
pub struct S2CoveringBind {
    cell_ids: Vec<u64>,
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
        use wkt::TryFromWkt;

        let geom = Geometry::try_from_wkt_str(&wkt_str)
            .map_err(|e| format!("WKT parse error: {}", e))?;

        let bbox = geom.bounding_rect().ok_or("Cannot compute bounding box")?;
        let region = s2::rect::Rect::from_degrees(
            s2::latlng::LatLng::from_degrees(bbox.min().y, bbox.min().x).unwrap(),
            s2::latlng::LatLng::from_degrees(bbox.max().y, bbox.max().x).unwrap(),
        );

        let coverer = s2::region::RegionCoverer::builder()
            .min_level(min_level as u8)
            .max_level(max_level as u8)
            .build();

        let covering = coverer.get_covering(&region);
        let cell_ids: Vec<u64> = covering.iter().map(|c| c.0).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("cell_id", DataType::Int64, false),
        ]));

        Ok(S2CoveringBind { cell_ids, schema })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(S2CoveringInit { done: false.into() })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        if func.init_data.done.swap(true, std::sync::atomic::Ordering::SeqCst) {
            output.set_len(0);
            return Ok(());
        }

        let ids: Vec<i64> = func.bind_data.cell_ids.iter().map(|c| *c as i64).collect();
        let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(Int64Array::from(ids)),
        ];
        let batch = RecordBatch::try_new(func.bind_data.schema.clone(), arrays)?;
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

// ─── Entrypoint ────────────────────────────────────────────────────────────

#[duckdb_entrypoint_c_api(ext_name = "raster")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    register_scalar_functions(&con)?;
    con.register_table_function::<RsMetadataVTab>("rs_metadata")?;
    con.register_table_function::<S2CoveringVTab>("s2_covering")?;
    Ok(())
}

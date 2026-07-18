//! DuckDB Raster Extension — S2 geography + ST_Transform + GeoTIFF
//! Ported from SedonaDB algorithms, pure Rust, zero system deps.

use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
#[cfg(feature = "loadable-extension")]
use duckdb::duckdb_entrypoint_c_api;
use duckdb::vscalar::arrow::{ArrowFunctionSignature, VArrowScalar};
use duckdb::vtab::arrow::record_batch_to_duckdb_data_chunk;
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::{Connection, Result};
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// ─── S2 Geography impls ─────────────────────────────────────────────────────

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

fn s2_cell_level_impl(cell_id: i64) -> i32 {
    s2::cellid::CellID(cell_id as u64).level() as i32
}

fn s2_to_geo_impl(cell_id: i64) -> String {
    let cell: s2::cell::Cell = s2::cellid::CellID(cell_id as u64).into();
    let center = cell.center();
    let ll: s2::latlng::LatLng = center.into();
    format!("POINT({:.7} {:.7})", ll.lng.deg(), ll.lat.deg())
}

fn s2_cell_to_hex_impl(cell_id: i64) -> String {
    s2::cellid::CellID(cell_id as u64).to_token()
}

fn s2_hex_to_cell_impl(hex: &str) -> i64 {
    s2::cellid::CellID::from_token(hex).0 as i64
}

fn s2_cell_vertex_impl(cell_id: i64, k: i32) -> String {
    let cell: s2::cell::Cell = s2::cellid::CellID(cell_id as u64).into();
    let v = cell.vertex(k as usize);
    let ll: s2::latlng::LatLng = v.into();
    format!("POINT({:.7} {:.7})", ll.lng.deg(), ll.lat.deg())
}

fn s2_cell_id_from_point_impl(wkt_str: &str) -> i64 {
    use wkt::TryFromWkt;
    let geom = match geo::geometry::Geometry::<f64>::try_from_wkt_str(wkt_str) {
        Ok(g) => g,
        Err(_) => return 0,
    };
    let pt = match geom {
        geo::geometry::Geometry::Point(p) => p,
        _ => return 0,
    };
    let ll = s2::latlng::LatLng::from_degrees(pt.y(), pt.x());
    let point = s2::point::Point::from(ll);
    s2::cellid::CellID::from(point).0 as i64
}

// ─── S2 scalar UDFs (new batch) ─────────────────────────────────────────────

pub struct S2CellLevel;
impl VArrowScalar for S2CellLevel {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let cell = input
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut b = arrow::array::Int32Array::builder(cell.len());
        for i in 0..cell.len() {
            b.append_value(s2_cell_level_impl(cell.value(i)));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Int64],
            DataType::Int32,
        )]
    }
}

pub struct S2ToGeo;
impl VArrowScalar for S2ToGeo {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let cell = input
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut b = StringBuilder::with_capacity(cell.len(), cell.len() * 40);
        for i in 0..cell.len() {
            b.append_value(s2_to_geo_impl(cell.value(i)));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Int64],
            DataType::Utf8,
        )]
    }
}

pub struct S2CellToHex;
impl VArrowScalar for S2CellToHex {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let cell = input
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut b = StringBuilder::with_capacity(cell.len(), cell.len() * 16);
        for i in 0..cell.len() {
            b.append_value(s2_cell_to_hex_impl(cell.value(i)));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Int64],
            DataType::Utf8,
        )]
    }
}

pub struct S2HexToCell;
impl VArrowScalar for S2HexToCell {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let hex = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = Int64Array::builder(hex.len());
        for i in 0..hex.len() {
            b.append_value(s2_hex_to_cell_impl(hex.value(i)));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Int64,
        )]
    }
}

pub struct S2CellVertex;
impl VArrowScalar for S2CellVertex {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let cell = input
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let k = input
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        let mut b = StringBuilder::with_capacity(cell.len(), cell.len() * 40);
        for i in 0..cell.len() {
            b.append_value(s2_cell_vertex_impl(cell.value(i), k.value(i)));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Int64, DataType::Int32],
            DataType::Utf8,
        )]
    }
}

pub struct S2CellIdFromPoint;
impl VArrowScalar for S2CellIdFromPoint {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let wkt = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = Int64Array::builder(wkt.len());
        for i in 0..wkt.len() {
            b.append_value(s2_cell_id_from_point_impl(wkt.value(i)));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Int64,
        )]
    }
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
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::{LazyLock, Mutex};
    use tiff::decoder::{Decoder, DecodingResult};
    use tiff::tags::Tag;

    /// Cached raster data — avoids re-opening TIFF on every function call
    struct CachedRaster {
        pixels: Vec<f64>,
        width: u32,
        height: u32,
        bands: u32,
        geo_transform: (f64, f64, f64, f64, f64, f64),
        nodata: f64,
        crs: String,
    }

    static CACHE: LazyLock<Mutex<HashMap<String, CachedRaster>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    fn load_raster(path: &str) -> Result<&CachedRaster, String> {
        // Use a block to limit lock scope
        {
            let cache = CACHE.lock().map_err(|e| format!("Lock: {}", e))?;
            if cache.contains_key(path) {
                // SAFETY: we return a reference that lives as long as the cache.
                // This works because CACHE is static and we know the entry exists.
                // But Rust borrow checker won't allow returning a ref from the Mutex guard.
                // So we use unsafe to work around this.
                return Ok(unsafe { &*(cache.get(path).unwrap() as *const CachedRaster) });
            }
        }

        // Load from file
        let file = File::open(path).map_err(|e| format!("Cannot open: {}", e))?;
        let reader = BufReader::new(file);
        let mut decoder = Decoder::new(reader).map_err(|e| format!("TIFF: {}", e))?;
        let (w, h) = decoder.dimensions().map_err(|e| format!("{}", e))?;
        let bands = decoder
            .find_tag_unsigned::<u32>(Tag::SamplesPerPixel)
            .ok()
            .flatten()
            .unwrap_or(1);

        // GeoTransform
        let tiepoint = decoder
            .get_tag_f64_vec(Tag::ModelTiepointTag)
            .unwrap_or(vec![0.0; 6]);
        let scale = decoder
            .get_tag_f64_vec(Tag::ModelPixelScaleTag)
            .unwrap_or(vec![1.0, 1.0, 0.0]);
        let gt = (
            tiepoint.get(3).copied().unwrap_or(0.0),
            scale.get(0).copied().unwrap_or(1.0),
            0.0,
            tiepoint.get(4).copied().unwrap_or(0.0),
            0.0,
            -scale.get(1).copied().unwrap_or(1.0),
        );

        // NoData
        let nodata = if let Ok(s) = decoder.get_tag_ascii_string(Tag::Unknown(42113)) {
            s.parse::<f64>().unwrap_or(f64::NAN)
        } else if let Ok(meta) = decoder.get_tag_ascii_string(Tag::Unknown(42112)) {
            let mut nd = f64::NAN;
            for line in meta.lines() {
                if (line.contains("NODATA") || line.contains("NoData")) && nd.is_nan() {
                    if let Some(v) = line.split('=').nth(1) {
                        nd = v.trim().parse::<f64>().unwrap_or(f64::NAN);
                    }
                }
            }
            nd
        } else {
            f64::NAN
        };

        // CRS from GeoKeyDirectory
        let crs = read_crs(&mut decoder);

        // Pixels
        let img = decoder.read_image().map_err(|e| format!("Read: {}", e))?;
        let np = (w as usize) * (h as usize);
        let pixels: Vec<f64> = match img {
            DecodingResult::F64(data) => data[..np.min(data.len())].to_vec(),
            DecodingResult::F32(data) => data.iter().take(np).map(|&v| v as f64).collect(),
            DecodingResult::U16(data) => data.iter().take(np).map(|&v| v as f64).collect(),
            DecodingResult::U8(data) => data.iter().take(np).map(|&v| v as f64).collect(),
            _ => return Err("Unsupported pixel type".into()),
        };

        let raster = CachedRaster {
            pixels,
            width: w,
            height: h,
            bands,
            geo_transform: gt,
            nodata,
            crs,
        };
        let mut cache = CACHE.lock().map_err(|e| format!("Lock: {}", e))?;
        cache.insert(path.to_string(), raster);
        Ok(unsafe { &*(cache.get(path).unwrap() as *const CachedRaster) })
    }

    fn read_crs(decoder: &mut Decoder<BufReader<File>>) -> String {
        // Try to get GeoKeyDirectoryTag (34735)
        if let Ok(keys) = decoder.get_tag_u16_vec(Tag::GeoKeyDirectoryTag) {
            if keys.len() >= 4 {
                let num_keys = keys[3] as usize;
                for i in 0..num_keys {
                    let base = 4 + i * 4;
                    if base + 3 < keys.len() {
                        let key_id = keys[base];
                        if key_id == 3072 {
                            // ProjectedCRSGeoKey
                            return format!("EPSG:{}", keys[base + 3]);
                        }
                        if key_id == 2048 {
                            // GeographicCRSGeoKey
                            return format!("EPSG:{}", keys[base + 3]);
                        }
                    }
                }
            }
        }
        "See GeoTIFF tags".to_string()
    }

    fn get_cached(path: &str) -> Result<&CachedRaster, String> {
        let cache = CACHE.lock().map_err(|e| format!("Lock: {}", e))?;
        if let Some(r) = cache.get(path) {
            Ok(unsafe { &*(r as *const CachedRaster) })
        } else {
            drop(cache);
            load_raster(path)
        }
    }

    pub fn read_dimensions(path: &str) -> Result<(u32, u32), String> {
        let r = get_cached(path)?;
        Ok((r.width, r.height))
    }

    pub fn read_band_count(path: &str) -> Result<u32, String> {
        let r = get_cached(path)?;
        Ok(r.bands)
    }

    pub fn read_width(path: &str) -> Result<u32, String> {
        Ok(get_cached(path)?.width)
    }

    pub fn read_height(path: &str) -> Result<u32, String> {
        Ok(get_cached(path)?.height)
    }

    pub fn read_nodata(path: &str, _band: u32) -> Result<f64, String> {
        Ok(get_cached(path)?.nodata)
    }

    pub fn read_geo_transform(path: &str) -> Result<(f64, f64, f64, f64, f64, f64), String> {
        Ok(get_cached(path)?.geo_transform)
    }

    pub fn read_scale_x(path: &str) -> Result<f64, String> {
        Ok(get_cached(path)?.geo_transform.1)
    }

    pub fn read_scale_y(path: &str) -> Result<f64, String> {
        Ok(get_cached(path)?.geo_transform.5.abs())
    }

    pub fn read_crs_str(path: &str) -> Result<String, String> {
        Ok(get_cached(path)?.crs.clone())
    }

    pub fn pixel_to_world(path: &str, col: f64, row: f64) -> Result<(f64, f64), String> {
        let gt = get_cached(path)?.geo_transform;
        let x = gt.0 + col * gt.1 + row * gt.2;
        let y = gt.3 + col * gt.4 + row * gt.5;
        Ok((x, y))
    }

    pub fn world_to_pixel(path: &str, x: f64, y: f64) -> Result<(f64, f64), String> {
        let gt = get_cached(path)?.geo_transform;
        let det = gt.1 * gt.5 - gt.2 * gt.4;
        if det.abs() < 1e-12 {
            return Err("Singular transform".into());
        }
        let col = (gt.5 * (x - gt.0) - gt.2 * (y - gt.3)) / det;
        let row = (-gt.4 * (x - gt.0) + gt.1 * (y - gt.3)) / det;
        Ok((col, row))
    }

    pub fn read_all_pixels(path: &str, _band: u32) -> Result<Vec<f64>, String> {
        Ok(get_cached(path)?.pixels.clone())
    }

    pub fn read_pixel(path: &str, _band: u32, col: u32, row: u32) -> Result<f64, String> {
        let r = get_cached(path)?;
        if col >= r.width || row >= r.height {
            return Err("Pixel out of bounds".into());
        }
        let idx = (row as usize) * (r.width as usize) + (col as usize);
        Ok(r.pixels.get(idx).copied().unwrap_or(f64::NAN))
    }

    pub fn read_metadata(path: &str) -> Result<(u32, u32, u32, String), String> {
        let r = get_cached(path)?;
        Ok((r.width, r.height, r.bands, r.crs.clone()))
    }
}

// ─── Remaining VArrowScalar implementations ────────────────────────────────

pub struct S2CellId;
impl VArrowScalar for S2CellId {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let lat = input
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let lon = input
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let lvl = input
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut b = Int64Array::builder(lat.len());
        for i in 0..lat.len() {
            b.append_value(s2_cell_id_impl(lat.value(i), lon.value(i), lvl.value(i)));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Float64, DataType::Float64, DataType::Int64],
            DataType::Int64,
        )]
    }
}

pub struct S2Contains;
impl VArrowScalar for S2Contains {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let cell = input
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let lat = input
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let lon = input
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut b = BooleanArray::builder(lat.len());
        for i in 0..lat.len() {
            b.append_value(s2_contains_impl(cell.value(i), lat.value(i), lon.value(i)));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Int64, DataType::Float64, DataType::Float64],
            DataType::Boolean,
        )]
    }
}

pub struct S2Distance;
impl VArrowScalar for S2Distance {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let c1 = input
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let c2 = input
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut b = Float64Array::builder(c1.len());
        for i in 0..c1.len() {
            b.append_value(s2_distance_meters_impl(c1.value(i), c2.value(i)));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Int64, DataType::Int64],
            DataType::Float64,
        )]
    }
}

pub struct S2Area;
impl VArrowScalar for S2Area {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let cell = input
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let lvl = input
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        let mut b = Float64Array::builder(cell.len());
        for i in 0..cell.len() {
            b.append_value(s2_area_m2_impl(cell.value(i), lvl.value(i)));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Int64, DataType::Int32],
            DataType::Float64,
        )]
    }
}

pub struct S2Parent;
impl VArrowScalar for S2Parent {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let cell = input
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let lvl = input
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut b = Int64Array::builder(cell.len());
        for i in 0..cell.len() {
            b.append_value(s2_parent_impl(cell.value(i), lvl.value(i)));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Int64, DataType::Int64],
            DataType::Int64,
        )]
    }
}

pub struct StTransformCoords;
impl VArrowScalar for StTransformCoords {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let x = input
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let y = input
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let from = input
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let to = input
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = StringBuilder::with_capacity(x.len(), x.len() * 20);
        for i in 0..x.len() {
            b.append_value(st_transform_coords_impl(
                x.value(i),
                y.value(i),
                from.value(i),
                to.value(i),
            ));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![
                DataType::Float64,
                DataType::Float64,
                DataType::Utf8,
                DataType::Utf8,
            ],
            DataType::Utf8,
        )]
    }
}

pub struct StTransform;
impl VArrowScalar for StTransform {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let wkt = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let from = input
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let to = input
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = StringBuilder::with_capacity(wkt.len(), wkt.len() * 100);
        for i in 0..wkt.len() {
            b.append_value(st_transform_impl(wkt.value(i), from.value(i), to.value(i)));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8, DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
        )]
    }
}

pub struct RsValue;
impl VArrowScalar for RsValue {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let band = input
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        let col = input
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        let row = input
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        let mut b = Float64Array::builder(path.len());
        for i in 0..path.len() {
            b.append_value(
                raster::read_pixel(
                    path.value(i),
                    band.value(i).max(1) as u32,
                    col.value(i).max(0) as u32,
                    row.value(i).max(0) as u32,
                )
                .unwrap_or(f64::NAN),
            );
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![
                DataType::Utf8,
                DataType::Int32,
                DataType::Int32,
                DataType::Int32,
            ],
            DataType::Float64,
        )]
    }
}

pub struct RsBandCount;
impl VArrowScalar for RsBandCount {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = arrow::array::Int32Array::builder(path.len());
        for i in 0..path.len() {
            b.append_value(raster::read_band_count(path.value(i)).unwrap_or(0) as i32);
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Int32,
        )]
    }
}

pub struct RsWidth;
impl VArrowScalar for RsWidth {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = arrow::array::Int32Array::builder(path.len());
        for i in 0..path.len() {
            b.append_value(raster::read_width(path.value(i)).unwrap_or(0) as i32);
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Int32,
        )]
    }
}

pub struct RsHeight;
impl VArrowScalar for RsHeight {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = arrow::array::Int32Array::builder(path.len());
        for i in 0..path.len() {
            b.append_value(raster::read_height(path.value(i)).unwrap_or(0) as i32);
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Int32,
        )]
    }
}

pub struct RsNodata;
impl VArrowScalar for RsNodata {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let band = input
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        let mut b = Float64Array::builder(path.len());
        for i in 0..path.len() {
            b.append_value(
                raster::read_nodata(path.value(i), band.value(i).max(1) as u32).unwrap_or(f64::NAN),
            );
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8, DataType::Int32],
            DataType::Float64,
        )]
    }
}

pub struct RsGeoTransform;
impl VArrowScalar for RsGeoTransform {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = StringBuilder::with_capacity(path.len(), path.len() * 80);
        for i in 0..path.len() {
            let gt = raster::read_geo_transform(path.value(i))
                .unwrap_or((0.0, 1.0, 0.0, 0.0, 0.0, -1.0));
            b.append_value(format!(
                "{},{},{},{},{},{}",
                gt.0, gt.1, gt.2, gt.3, gt.4, gt.5
            ));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Utf8,
        )]
    }
}

pub struct RsPixelToWorld;
impl VArrowScalar for RsPixelToWorld {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col = input
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let row = input
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut b = StringBuilder::with_capacity(path.len(), path.len() * 30);
        for i in 0..path.len() {
            let (x, y) = raster::pixel_to_world(path.value(i), col.value(i), row.value(i))
                .unwrap_or((f64::NAN, f64::NAN));
            b.append_value(format!("POINT({:.7} {:.7})", x, y));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8, DataType::Float64, DataType::Float64],
            DataType::Utf8,
        )]
    }
}

pub struct RsWorldToPixel;
impl VArrowScalar for RsWorldToPixel {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let x = input
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let y = input
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut b = StringBuilder::with_capacity(path.len(), path.len() * 30);
        for i in 0..path.len() {
            let (col, row) = raster::world_to_pixel(path.value(i), x.value(i), y.value(i))
                .unwrap_or((f64::NAN, f64::NAN));
            b.append_value(format!("POINT({:.3} {:.3})", col, row));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8, DataType::Float64, DataType::Float64],
            DataType::Utf8,
        )]
    }
}

// ─── Raster convenience scalars ────────────────────────────────────────────

pub struct RsScaleX;
impl VArrowScalar for RsScaleX {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = Float64Array::builder(path.len());
        for i in 0..path.len() {
            b.append_value(raster::read_scale_x(path.value(i)).unwrap_or(1.0));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Float64,
        )]
    }
}

pub struct RsScaleY;
impl VArrowScalar for RsScaleY {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = Float64Array::builder(path.len());
        for i in 0..path.len() {
            b.append_value(raster::read_scale_y(path.value(i)).unwrap_or(1.0));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Float64,
        )]
    }
}

pub struct RsCrs;
impl VArrowScalar for RsCrs {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = StringBuilder::with_capacity(path.len(), 20);
        for i in 0..path.len() {
            b.append_value(raster::read_crs_str(path.value(i)).unwrap_or_default());
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Utf8,
        )]
    }
}

pub struct RsMin;
impl VArrowScalar for RsMin {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = Float64Array::builder(path.len());
        for i in 0..path.len() {
            let p = raster::read_all_pixels(path.value(i), 1).unwrap_or_default();
            b.append_value(p.iter().cloned().fold(f64::INFINITY, f64::min));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Float64,
        )]
    }
}

pub struct RsMax;
impl VArrowScalar for RsMax {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = Float64Array::builder(path.len());
        for i in 0..path.len() {
            let p = raster::read_all_pixels(path.value(i), 1).unwrap_or_default();
            b.append_value(p.iter().cloned().fold(f64::NEG_INFINITY, f64::max));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Float64,
        )]
    }
}

pub struct RsMean;
impl VArrowScalar for RsMean {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = Float64Array::builder(path.len());
        for i in 0..path.len() {
            let p = raster::read_all_pixels(path.value(i), 1).unwrap_or_default();
            let m = if p.is_empty() {
                f64::NAN
            } else {
                p.iter().sum::<f64>() / p.len() as f64
            };
            b.append_value(m);
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Float64,
        )]
    }
}

pub struct RsStddev;
impl VArrowScalar for RsStddev {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let path = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut b = Float64Array::builder(path.len());
        for i in 0..path.len() {
            let p = raster::read_all_pixels(path.value(i), 1).unwrap_or_default();
            let s = if p.is_empty() {
                f64::NAN
            } else {
                let mean = p.iter().sum::<f64>() / p.len() as f64;
                (p.iter().map(|&v| (v - mean).powi(2)).sum::<f64>() / p.len() as f64).sqrt()
            };
            b.append_value(s);
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Float64,
        )]
    }
}

// ─── Table Functions ────────────────────────────────────────────────────────

// rs_metadata
pub struct RsMetadataVTab;
#[repr(C)]
pub struct RsMetaInit {
    done: AtomicBool,
}
#[repr(C)]
pub struct RsMetaBind {
    row: Option<MetaRow>,
    schema: Arc<Schema>,
}
struct MetaRow {
    path: String,
    width: i32,
    height: i32,
    bands: i32,
    crs: String,
}

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
            row: Some(MetaRow {
                path,
                width: w as i32,
                height: h as i32,
                bands: b as i32,
                crs,
            }),
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
        Ok(RsMetaInit {
            done: AtomicBool::new(false),
        })
    }
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        if func.get_init_data().done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
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

// s2_covering
pub struct S2CoveringVTab;
#[repr(C)]
pub struct S2CoveringInit {
    done: AtomicBool,
}
#[repr(C)]
pub struct S2CoveringBind {
    cells: Vec<i64>,
}

fn covering_from_wkt(wkt_str: &str, min_level: i32, max_level: i32) -> Vec<i64> {
    use geo::algorithm::BoundingRect;
    use geo::Geometry;
    use s2::region::RegionCoverer;
    use wkt::TryFromWkt;
    let geom = match Geometry::<f64>::try_from_wkt_str(wkt_str) {
        Ok(g) => g,
        Err(_) => return vec![],
    };
    let bbox = match geom.bounding_rect() {
        Some(r) => r,
        None => return vec![],
    };
    let rect = s2::rect::Rect::from_degrees(bbox.min().y, bbox.min().x, bbox.max().y, bbox.max().x);
    let coverer = RegionCoverer {
        min_level: min_level.max(0) as u8,
        max_level: max_level.min(30) as u8,
        level_mod: 1,
        max_cells: 64,
    };
    let cu = coverer.covering(&rect);
    cu.0.iter().map(|cid| cid.0 as i64).collect()
}

impl VTab for S2CoveringVTab {
    type BindData = S2CoveringBind;
    type InitData = S2CoveringInit;
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("cell_id", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        let wkt = bind.get_parameter(0).to_string();
        let min_lvl: i32 = bind.get_parameter(1).to_int32();
        let max_lvl: i32 = bind.get_parameter(2).to_int32();
        Ok(S2CoveringBind {
            cells: covering_from_wkt(&wkt, min_lvl, max_lvl),
        })
    }
    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(S2CoveringInit {
            done: AtomicBool::new(false),
        })
    }
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        if func.get_init_data().done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        let bd = func.get_bind_data();
        let cell_ids: Int64Array = bd.cells.iter().map(|&c| c).collect();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "cell_id",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(cell_ids)],
        )?;
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

// s2_children
pub struct S2ChildrenVTab;
#[repr(C)]
pub struct S2ChildrenInit {
    done: AtomicBool,
}
#[repr(C)]
pub struct S2ChildrenBind {
    children: Vec<i64>,
}

impl VTab for S2ChildrenVTab {
    type BindData = S2ChildrenBind;
    type InitData = S2ChildrenInit;
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("child_id", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        let cell_id: i64 = bind.get_parameter(0).to_int64();
        let level: u64 = bind.get_parameter(1).to_int32() as u64;
        let cid = s2::cellid::CellID(cell_id as u64);
        let children: Vec<i64> = cid.child_iter_at_level(level).map(|c| c.0 as i64).collect();
        Ok(S2ChildrenBind { children })
    }
    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(S2ChildrenInit {
            done: AtomicBool::new(false),
        })
    }
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        if func.get_init_data().done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        let bd = func.get_bind_data();
        let ids: Int64Array = bd.children.iter().map(|&c| c).collect();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "child_id",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(ids)],
        )?;
        record_batch_to_duckdb_data_chunk(&batch, output)?;
        Ok(())
    }
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Bigint),
            LogicalTypeHandle::from(LogicalTypeId::Integer),
        ])
    }
}

// s2_cell_neighbors
pub struct S2NeighborsVTab;
#[repr(C)]
pub struct S2NeighborsInit {
    done: AtomicBool,
}
#[repr(C)]
pub struct S2NeighborsBind {
    neighbors: Vec<i64>,
}

impl VTab for S2NeighborsVTab {
    type BindData = S2NeighborsBind;
    type InitData = S2NeighborsInit;
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column(
            "neighbor_id",
            LogicalTypeHandle::from(LogicalTypeId::Bigint),
        );
        let cell_id: i64 = bind.get_parameter(0).to_int64();
        let cid = s2::cellid::CellID(cell_id as u64);
        let neighbors: Vec<i64> = cid.edge_neighbors().iter().map(|c| c.0 as i64).collect();
        Ok(S2NeighborsBind { neighbors })
    }
    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(S2NeighborsInit {
            done: AtomicBool::new(false),
        })
    }
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        if func.get_init_data().done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        let bd = func.get_bind_data();
        let ids: Int64Array = bd.neighbors.iter().map(|&c| c).collect();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "neighbor_id",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(ids)],
        )?;
        record_batch_to_duckdb_data_chunk(&batch, output)?;
        Ok(())
    }
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Bigint)])
    }
}

// rs_stats(path, band) → table(min, max, mean, stddev, count)
pub struct RsStatsVTab;
#[repr(C)]
pub struct RsStatsInit {
    done: AtomicBool,
}
#[repr(C)]
pub struct RsStatsBind {
    stats: (f64, f64, f64, f64, i64),
}

impl VTab for RsStatsVTab {
    type BindData = RsStatsBind;
    type InitData = RsStatsInit;
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("min", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("max", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("mean", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("stddev", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("count", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        let path = bind.get_parameter(0).to_string();
        let band = bind.get_parameter(1).to_int32().max(1) as u32;
        let pixels = raster::read_all_pixels(&path, band)?;
        let n = pixels.len() as f64;
        if n == 0.0 {
            return Ok(RsStatsBind {
                stats: (0.0, 0.0, 0.0, 0.0, 0),
            });
        }
        let min = pixels.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = pixels.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let sum: f64 = pixels.iter().sum();
        let mean = sum / n;
        let variance = pixels.iter().map(|&v| (v - mean).powi(2)).sum::<f64>() / n;
        Ok(RsStatsBind {
            stats: (min, max, mean, variance.sqrt(), pixels.len() as i64),
        })
    }
    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(RsStatsInit {
            done: AtomicBool::new(false),
        })
    }
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        if func.get_init_data().done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        let s = func.get_bind_data().stats;
        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(Float64Array::from(vec![s.0])),
            Arc::new(Float64Array::from(vec![s.1])),
            Arc::new(Float64Array::from(vec![s.2])),
            Arc::new(Float64Array::from(vec![s.3])),
            Arc::new(Int64Array::from(vec![s.4])),
        ];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("min", DataType::Float64, false),
                Field::new("max", DataType::Float64, false),
                Field::new("mean", DataType::Float64, false),
                Field::new("stddev", DataType::Float64, false),
                Field::new("count", DataType::Int64, false),
            ])),
            arrays,
        )?;
        record_batch_to_duckdb_data_chunk(&batch, output)?;
        Ok(())
    }
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Integer),
        ])
    }
}

// rs_histogram(path, band, bins) → table(value, count)
pub struct RsHistogramVTab;
#[repr(C)]
pub struct RsHistogramInit {
    done: AtomicBool,
}
#[repr(C)]
pub struct RsHistogramBind {
    values: Vec<f64>,
    counts: Vec<i64>,
}

impl VTab for RsHistogramVTab {
    type BindData = RsHistogramBind;
    type InitData = RsHistogramInit;
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("value", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("count", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        let path = bind.get_parameter(0).to_string();
        let band = bind.get_parameter(1).to_int32().max(1) as u32;
        let bins = bind.get_parameter(2).to_int32().max(2) as usize;
        let pixels = raster::read_all_pixels(&path, band)?;
        if pixels.is_empty() {
            return Ok(RsHistogramBind {
                values: vec![],
                counts: vec![],
            });
        }
        let min = pixels.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = pixels.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let width = (max - min) / bins as f64;
        let mut counts = vec![0i64; bins];
        let mut values: Vec<f64> = (0..bins).map(|i| min + (i as f64 + 0.5) * width).collect();
        for &p in &pixels {
            if p < min || p > max {
                continue;
            }
            let idx = ((p - min) / width) as usize;
            if idx < bins {
                counts[idx] += 1;
            }
        }
        Ok(RsHistogramBind { values, counts })
    }
    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(RsHistogramInit {
            done: AtomicBool::new(false),
        })
    }
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        if func.get_init_data().done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        let bd = func.get_bind_data();
        let vals: Float64Array = bd.values.iter().map(|&v| v).collect();
        let cnts: Int64Array = bd.counts.iter().map(|&c| c).collect();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Float64, false),
                Field::new("count", DataType::Int64, false),
            ])),
            vec![Arc::new(vals), Arc::new(cnts)],
        )?;
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

#[cfg(feature = "loadable-extension")]
#[duckdb_entrypoint_c_api(ext_name = "raster")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    // S2
    con.register_scalar_function::<S2CellId>("s2_cell_id")?;
    con.register_scalar_function::<S2Contains>("s2_contains")?;
    con.register_scalar_function::<S2Distance>("s2_distance_meters")?;
    con.register_scalar_function::<S2Area>("s2_area_m2")?;
    con.register_scalar_function::<S2Parent>("s2_parent")?;
    con.register_scalar_function::<S2CellLevel>("s2_cell_level")?;
    con.register_scalar_function::<S2ToGeo>("s2_to_geo")?;
    con.register_scalar_function::<S2CellToHex>("s2_cell_to_hex")?;
    con.register_scalar_function::<S2HexToCell>("s2_hex_to_cell")?;
    con.register_scalar_function::<S2CellVertex>("s2_cell_vertex")?;
    con.register_scalar_function::<S2CellIdFromPoint>("s2_cell_id_from_point")?;
    con.register_table_function::<S2CoveringVTab>("s2_covering")?;
    con.register_table_function::<S2ChildrenVTab>("s2_children")?;
    con.register_table_function::<S2NeighborsVTab>("s2_cell_neighbors")?;
    // ST_Transform
    con.register_scalar_function::<StTransformCoords>("st_transform_coords")?;
    con.register_scalar_function::<StTransform>("st_transform")?;
    // Raster
    con.register_scalar_function::<RsValue>("rs_value")?;
    con.register_scalar_function::<RsBandCount>("rs_band_count")?;
    con.register_scalar_function::<RsWidth>("rs_width")?;
    con.register_scalar_function::<RsHeight>("rs_height")?;
    con.register_scalar_function::<RsNodata>("rs_nodata")?;
    con.register_scalar_function::<RsGeoTransform>("rs_geo_transform")?;
    con.register_scalar_function::<RsPixelToWorld>("rs_pixel_to_world")?;
    con.register_scalar_function::<RsWorldToPixel>("rs_world_to_pixel")?;
    con.register_scalar_function::<RsScaleX>("rs_scale_x")?;
    con.register_scalar_function::<RsScaleY>("rs_scale_y")?;
    con.register_scalar_function::<RsCrs>("rs_crs")?;
    con.register_scalar_function::<RsMin>("rs_min")?;
    con.register_scalar_function::<RsMax>("rs_max")?;
    con.register_scalar_function::<RsMean>("rs_mean")?;
    con.register_scalar_function::<RsStddev>("rs_stddev")?;
    con.register_table_function::<RsMetadataVTab>("rs_metadata")?;
    con.register_table_function::<RsStatsVTab>("rs_stats")?;
    con.register_table_function::<RsHistogramVTab>("rs_histogram")?;
    Ok(())
}

//! GeoTIFF Raster — load, cache (DashMap), metadata, pixel read.
//! DashMap-based concurrent cache, no Mutex contention.

use dashmap::DashMap;
use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc, LazyLock};
use tiff::decoder::{Decoder, DecodingResult};
use tiff::tags::Tag;

/// Cached raster data — avoids re-opening TIFF on every function call
pub struct CachedRaster {
    /// All pixels, band-interleaved (chunky): pixel at (col,row,band) = pixels[(row*w+col)*bands+band]
    pixels: Vec<f64>,
    pub width: u32,
    pub height: u32,
    pub bands: u32,
    pub geo_transform: (f64, f64, f64, f64, f64, f64),
    pub nodata: f64,
    pub crs: String,
}

static CACHE: LazyLock<DashMap<String, Arc<CachedRaster>>> = LazyLock::new(DashMap::new);

fn load_raster(path: &str) -> Result<Arc<CachedRaster>, String> {
    let file = File::open(path).map_err(|e| format!("Cannot open: {}", e))?;
    let reader = BufReader::new(file);
    let mut decoder = Decoder::new(reader).map_err(|e| format!("TIFF: {}", e))?;
    let (w, h) = decoder.dimensions().map_err(|e| format!("{}", e))?;
    let bands = decoder
        .find_tag_unsigned::<u32>(Tag::SamplesPerPixel)
        .ok()
        .flatten()
        .unwrap_or(1);

    // GeoTransform from ModelTiepoint + ModelPixelScale
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

    let raster = Arc::new(CachedRaster {
        pixels,
        width: w,
        height: h,
        bands,
        geo_transform: gt,
        nodata,
        crs,
    });
    CACHE.insert(path.to_string(), Arc::clone(&raster));
    Ok(raster)
}

fn read_crs(decoder: &mut Decoder<BufReader<File>>) -> String {
    if let Ok(keys) = decoder.get_tag_u16_vec(Tag::GeoKeyDirectoryTag) {
        if keys.len() >= 4 {
            let num_keys = keys[3] as usize;
            for i in 0..num_keys {
                let base = 4 + i * 4;
                if base + 3 < keys.len() {
                    let key_id = keys[base];
                    if key_id == 3072 {
                        return format!("EPSG:{}", keys[base + 3]);
                    } // ProjectedCRSGeoKey
                    if key_id == 2048 {
                        return format!("EPSG:{}", keys[base + 3]);
                    } // GeographicCRSGeoKey
                }
            }
        }
    }
    "See GeoTIFF tags".to_string()
}

fn get_cached(path: &str) -> Result<Arc<CachedRaster>, String> {
    if let Some(r) = CACHE.get(path) {
        Ok(Arc::clone(&r))
    } else {
        load_raster(path)
    }
}

// ─── Public API ────────────────────────────────────────────────────────────

pub fn read_pixel(path: &str, band: u32, col: u32, row: u32) -> Result<f64, String> {
    let r = get_cached(path)?;
    if col >= r.width || row >= r.height {
        return Err("Pixel out of bounds".into());
    }
    let b = (band - 1).min(r.bands.saturating_sub(1));
    let idx = ((row as usize) * (r.width as usize) + (col as usize)) * (r.bands as usize) + (b as usize);
    Ok(r.pixels.get(idx).copied().unwrap_or(f64::NAN))
}

pub fn all_pixels(path: &str, band: u32) -> Result<Vec<f64>, String> {
    let r = get_cached(path)?;
    let b = (band - 1).min(r.bands.saturating_sub(1)) as usize;
    let bands = r.bands as usize;
    let w = r.width as usize;
    let h = r.height as usize;
    let pixels = Arc::clone(&r);
    // need local copy for the closure
    drop(r);
    Ok((0..h).flat_map(|row| {
        (0..w).map(move |col| {
            let idx = (row * w + col) * bands + b;
            pixels.pixels.get(idx).copied().unwrap_or(f64::NAN)
        })
    }).collect())
}

pub fn band_count(path: &str) -> Result<u32, String> {
    Ok(get_cached(path)?.bands)
}
pub fn width(path: &str) -> Result<u32, String> {
    Ok(get_cached(path)?.width)
}
pub fn height(path: &str) -> Result<u32, String> {
    Ok(get_cached(path)?.height)
}
pub fn nodata(path: &str, _band: u32) -> Result<f64, String> {
    Ok(get_cached(path)?.nodata)
}
pub fn geo_transform(path: &str) -> Result<(f64, f64, f64, f64, f64, f64), String> {
    Ok(get_cached(path)?.geo_transform)
}
pub fn scale_x(path: &str) -> Result<f64, String> {
    Ok(get_cached(path)?.geo_transform.1)
}
pub fn scale_y(path: &str) -> Result<f64, String> {
    Ok(get_cached(path)?.geo_transform.5.abs())
}
pub fn crs(path: &str) -> Result<String, String> {
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

pub fn read_metadata(path: &str) -> Result<(u32, u32, u32, String), String> {
    let r = get_cached(path)?;
    Ok((r.width, r.height, r.bands, r.crs.clone()))
}

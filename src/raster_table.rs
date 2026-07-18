//! Raster table functions — rs_metadata, rs_stats, rs_histogram.

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::arrow::record_batch_to_duckdb_data_chunk;
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::Result;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::raster;

// ─── rs_metadata ────────────────────────────────────────────────────────────

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

// ─── rs_stats ───────────────────────────────────────────────────────────────

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
        let pixels = raster::all_pixels(&path, band)?;
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

// ─── rs_histogram ───────────────────────────────────────────────────────────

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
        let pixels = raster::all_pixels(&path, band)?;
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
        let values: Vec<f64> = (0..bins).map(|i| min + (i as f64 + 0.5) * width).collect();
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

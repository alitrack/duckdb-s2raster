//! Raster scalar UDFs — VArrowScalar implementations.

use arrow::array::StringArray;
use arrow::array::{Array, Float64Array, RecordBatch, StringBuilder};
use arrow::datatypes::DataType;
use duckdb::vscalar::arrow::{ArrowFunctionSignature, VArrowScalar};
use duckdb::Result;
use std::error::Error;
use std::sync::Arc;

use crate::raster;

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
            b.append_value(raster::band_count(path.value(i)).unwrap_or(0) as i32);
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
            b.append_value(raster::width(path.value(i)).unwrap_or(0) as i32);
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
            b.append_value(raster::height(path.value(i)).unwrap_or(0) as i32);
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
                raster::nodata(path.value(i), band.value(i).max(1) as u32).unwrap_or(f64::NAN),
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
            let gt =
                raster::geo_transform(path.value(i)).unwrap_or((0.0, 1.0, 0.0, 0.0, 0.0, -1.0));
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
            b.append_value(raster::scale_x(path.value(i)).unwrap_or(1.0));
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
            b.append_value(raster::scale_y(path.value(i)).unwrap_or(1.0));
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
            b.append_value(raster::crs(path.value(i)).unwrap_or_default());
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
            let p = raster::all_pixels(path.value(i), 1).unwrap_or_default();
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
            let p = raster::all_pixels(path.value(i), 1).unwrap_or_default();
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
            let p = raster::all_pixels(path.value(i), 1).unwrap_or_default();
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
            let p = raster::all_pixels(path.value(i), 1).unwrap_or_default();
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

//! S2 scalar UDFs — VArrowScalar implementations.

use arrow::array::Array;
use arrow::array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringBuilder};
use arrow::datatypes::DataType;
use duckdb::vscalar::arrow::{ArrowFunctionSignature, VArrowScalar};
use duckdb::Result;
use std::error::Error;
use std::sync::Arc;

use crate::s2_impl::*;

// ─── Simple 1-arg scalars ──────────────────────────────────────────────────

pub struct S2CellLevel;
impl VArrowScalar for S2CellLevel {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let a0 = input
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut b = arrow::array::Int32Array::builder(a0.len());
        for i in 0..a0.len() {
            b.append_value(s2_cell_level_impl(a0.value(i)));
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
        let a0 = input
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut b = StringBuilder::with_capacity(a0.len(), a0.len() * 40);
        for i in 0..a0.len() {
            b.append_value(s2_to_geo_impl(a0.value(i)));
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
        let a0 = input
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut b = StringBuilder::with_capacity(a0.len(), a0.len() * 16);
        for i in 0..a0.len() {
            b.append_value(s2_cell_to_hex_impl(a0.value(i)));
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
        let a0 = input
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let mut b = Int64Array::builder(a0.len());
        for i in 0..a0.len() {
            b.append_value(s2_hex_to_cell_impl(a0.value(i)).unwrap_or(0));
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
        let a0 = input
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let a1 = input
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        let mut b = StringBuilder::with_capacity(a0.len(), a0.len() * 40);
        for i in 0..a0.len() {
            b.append_value(s2_cell_vertex_impl(a0.value(i), a1.value(i)).unwrap_or_default());
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
        let a0 = input
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let mut b = Int64Array::builder(a0.len());
        for i in 0..a0.len() {
            b.append_value(s2_cell_id_from_point_impl(a0.value(i)));
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

// ─── Original S2 scalars (multi-arg) ───────────────────────────────────────

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

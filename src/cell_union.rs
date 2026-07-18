//! S2 CellUnion — BLOB serialization helpers + VArrowScalar implementations.
//! Enables aggregate-like operations via DuckDB string_agg + s2_cell_union_pack.

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::DataType;
use duckdb::vscalar::arrow::{ArrowFunctionSignature, VArrowScalar};
use duckdb::Result;
use std::error::Error;
use std::sync::Arc;

// ─── BLOB codec ─────────────────────────────────────────────────────────────

pub fn pack_cell_union(cells: &[i64]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4 + cells.len() * 8);
    bytes.extend_from_slice(&(cells.len() as u32).to_le_bytes());
    for &c in cells {
        bytes.extend_from_slice(&(c as u64).to_le_bytes());
    }
    bytes
}

pub fn unpack_cell_union(blob: &[u8]) -> Vec<u64> {
    if blob.len() < 4 {
        return vec![];
    }
    let count = u32::from_le_bytes([blob[0], blob[1], blob[2], blob[3]]) as usize;
    let mut cells = Vec::with_capacity(count);
    for i in 0..count {
        let off = 4 + i * 8;
        if off + 8 > blob.len() {
            break;
        }
        cells.push(u64::from_le_bytes([
            blob[off],
            blob[off + 1],
            blob[off + 2],
            blob[off + 3],
            blob[off + 4],
            blob[off + 5],
            blob[off + 6],
            blob[off + 7],
        ]));
    }
    cells
}

// ─── s2_cell_union_pack(csv_hex_tokens) → BLOB ─────────────────────────────

pub struct S2CellUnionPack;
impl VArrowScalar for S2CellUnionPack {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let csv = input
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let mut b = arrow::array::BinaryBuilder::with_capacity(csv.len(), csv.len() * 64);
        for i in 0..csv.len() {
            let cells: Vec<i64> = csv
                .value(i)
                .split(',')
                .filter_map(|s| {
                    let s = s.trim();
                    if s.is_empty() {
                        None
                    } else {
                        Some(s2::cellid::CellID::from_token(s).0 as i64)
                    }
                })
                .collect();
            b.append_value(&pack_cell_union(&cells));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Binary,
        )]
    }
}

// ─── s2_cell_union_contains(cu_blob, cell_id) → bool ────────────────────────

pub struct S2CellUnionContains;
impl VArrowScalar for S2CellUnionContains {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let blob = input
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        let cell = input
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut b = BooleanArray::builder(blob.len());
        for i in 0..blob.len() {
            let cells = unpack_cell_union(blob.value(i));
            let cid = s2::cellid::CellID(cell.value(i) as u64);
            let cu = s2::cellunion::CellUnion(cells.into_iter().map(s2::cellid::CellID).collect());
            b.append_value(cu.contains_cellid(&cid));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Binary, DataType::Int64],
            DataType::Boolean,
        )]
    }
}

// ─── s2_cell_union_intersects(blob1, blob2) → bool ──────────────────────────

pub struct S2CellUnionIntersects;
impl VArrowScalar for S2CellUnionIntersects {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let b1 = input
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        let b2 = input
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        let mut out = BooleanArray::builder(b1.len());
        for i in 0..b1.len() {
            let c1 = unpack_cell_union(b1.value(i));
            let c2 = unpack_cell_union(b2.value(i));
            let cu1 = s2::cellunion::CellUnion(c1.into_iter().map(s2::cellid::CellID).collect());
            let cu2 = s2::cellunion::CellUnion(c2.into_iter().map(s2::cellid::CellID).collect());
            out.append_value(cu1.intersects_cell_union(&cu2));
        }
        Ok(Arc::new(out.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Binary, DataType::Binary],
            DataType::Boolean,
        )]
    }
}

// ─── s2_cell_union_area(cu_blob) → m² ───────────────────────────────────────

pub struct S2CellUnionArea;
impl VArrowScalar for S2CellUnionArea {
    type State = ();
    fn invoke(_: &(), input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let blob = input
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        let mut b = Float64Array::builder(blob.len());
        for i in 0..blob.len() {
            let cells = unpack_cell_union(blob.value(i));
            let cu = s2::cellunion::CellUnion(cells.into_iter().map(s2::cellid::CellID).collect());
            b.append_value(cu.approx_area() * 6_371_009.0_f64.powi(2));
        }
        Ok(Arc::new(b.finish()))
    }
    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Binary],
            DataType::Float64,
        )]
    }
}

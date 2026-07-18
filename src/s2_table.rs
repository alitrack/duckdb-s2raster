//! S2 table functions — VTab implementations (covering, children, neighbors, interior_covering).

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::arrow::record_batch_to_duckdb_data_chunk;
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::Result;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// ─── s2_covering ────────────────────────────────────────────────────────────

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
    coverer
        .covering(&rect)
        .0
        .iter()
        .map(|cid| cid.0 as i64)
        .collect()
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
        let ids: Int64Array = bd.cells.iter().map(|&c| c).collect();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "cell_id",
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
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Integer),
            LogicalTypeHandle::from(LogicalTypeId::Integer),
        ])
    }
}

// ─── s2_children ────────────────────────────────────────────────────────────

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
        Ok(S2ChildrenBind {
            children: cid.child_iter_at_level(level).map(|c| c.0 as i64).collect(),
        })
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

// ─── s2_cell_neighbors ──────────────────────────────────────────────────────

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
        Ok(S2NeighborsBind {
            neighbors: cid.edge_neighbors().iter().map(|c| c.0 as i64).collect(),
        })
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

// ─── s2_interior_covering ───────────────────────────────────────────────────

pub struct S2InteriorCoveringVTab;
#[repr(C)]
pub struct S2InteriorCoveringInit {
    done: AtomicBool,
}
#[repr(C)]
pub struct S2InteriorCoveringBind {
    cells: Vec<i64>,
}

fn interior_covering_from_wkt(
    wkt_str: &str,
    min_level: i32,
    max_level: i32,
    max_cells: usize,
) -> Vec<i64> {
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
        max_cells,
    };
    coverer
        .interior_covering(&rect)
        .0
        .iter()
        .map(|cid| cid.0 as i64)
        .collect()
}

impl VTab for S2InteriorCoveringVTab {
    type BindData = S2InteriorCoveringBind;
    type InitData = S2InteriorCoveringInit;
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("cell_id", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        let wkt = bind.get_parameter(0).to_string();
        let min_lvl: i32 = bind.get_parameter(1).to_int32();
        let max_lvl: i32 = bind.get_parameter(2).to_int32();
        let max_cells: usize = bind.get_parameter(3).to_int32().max(4) as usize;
        Ok(S2InteriorCoveringBind {
            cells: interior_covering_from_wkt(&wkt, min_lvl, max_lvl, max_cells),
        })
    }
    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(S2InteriorCoveringInit {
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
        let ids: Int64Array = bd.cells.iter().map(|&c| c).collect();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "cell_id",
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
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Integer),
            LogicalTypeHandle::from(LogicalTypeId::Integer),
            LogicalTypeHandle::from(LogicalTypeId::Integer),
        ])
    }
}

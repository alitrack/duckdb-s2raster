//! Integration tests for duckdb_raster extension.
//! Run with: cargo test --features test-bundled --test integration_test

use duckdb::Connection;
use std::error::Error;

fn setup_conn() -> Result<Connection, Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    // Register all extension functions manually (bundled feature = no auto-load)
    conn.register_scalar_function::<duckdb_raster::S2CellId>("s2_cell_id")?;
    conn.register_scalar_function::<duckdb_raster::S2Contains>("s2_contains")?;
    conn.register_scalar_function::<duckdb_raster::S2Distance>("s2_distance_meters")?;
    conn.register_scalar_function::<duckdb_raster::S2Area>("s2_area_m2")?;
    conn.register_scalar_function::<duckdb_raster::S2Parent>("s2_parent")?;
    conn.register_scalar_function::<duckdb_raster::S2CellLevel>("s2_cell_level")?;
    conn.register_scalar_function::<duckdb_raster::S2ToGeo>("s2_to_geo")?;
    conn.register_scalar_function::<duckdb_raster::S2CellToHex>("s2_cell_to_hex")?;
    conn.register_scalar_function::<duckdb_raster::S2HexToCell>("s2_hex_to_cell")?;
    conn.register_scalar_function::<duckdb_raster::StTransformCoords>("st_transform_coords")?;
    conn.register_scalar_function::<duckdb_raster::StTransform>("st_transform")?;
    conn.register_scalar_function::<duckdb_raster::RsValue>("rs_value")?;
    conn.register_table_function::<duckdb_raster::RsMetadataVTab>("rs_metadata")?;
    conn.register_table_function::<duckdb_raster::S2CoveringVTab>("s2_covering")?;
    conn.register_table_function::<duckdb_raster::S2ChildrenVTab>("s2_children")?;
    conn.register_table_function::<duckdb_raster::S2NeighborsVTab>("s2_cell_neighbors")?;
    conn.register_table_function::<duckdb_raster::RsStatsVTab>("rs_stats")?;
    conn.register_table_function::<duckdb_raster::RsHistogramVTab>("rs_histogram")?;
    conn.register_scalar_function::<duckdb_raster::RsBandCount>("rs_band_count")?;
    conn.register_scalar_function::<duckdb_raster::RsWidth>("rs_width")?;
    conn.register_scalar_function::<duckdb_raster::RsHeight>("rs_height")?;
    conn.register_scalar_function::<duckdb_raster::RsGeoTransform>("rs_geo_transform")?;
    conn.register_scalar_function::<duckdb_raster::RsPixelToWorld>("rs_pixel_to_world")?;
    conn.register_scalar_function::<duckdb_raster::RsWorldToPixel>("rs_world_to_pixel")?;
    conn.register_scalar_function::<duckdb_raster::S2CellVertex>("s2_cell_vertex")?;
    conn.register_scalar_function::<duckdb_raster::S2CellIdFromPoint>("s2_cell_id_from_point")?;
    Ok(conn)
}

#[test]
fn test_s2_cell_id() {
    let conn = setup_conn().unwrap();
    let result: i64 = conn
        .query_row("SELECT s2_cell_id(39.9, 116.4, 10)", [], |row| row.get(0))
        .unwrap();
    assert!(result > 0, "Cell ID should be positive: {}", result);
}

#[test]
fn test_s2_contains() {
    let conn = setup_conn().unwrap();
    let cell: i64 = conn
        .query_row("SELECT s2_cell_id(39.9, 116.4, 10)", [], |row| row.get(0))
        .unwrap();
    let contains: bool = conn
        .query_row(
            &format!("SELECT s2_contains({}, 39.9001, 116.4001)", cell),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(contains);
}

#[test]
fn test_s2_distance() {
    let conn = setup_conn().unwrap();
    let c1: i64 = conn
        .query_row("SELECT s2_cell_id(39.9, 116.4, 8)", [], |row| row.get(0))
        .unwrap();
    let c2: i64 = conn
        .query_row("SELECT s2_cell_id(40.0, 117.0, 8)", [], |row| row.get(0))
        .unwrap();
    let dist: f64 = conn
        .query_row(
            &format!("SELECT s2_distance_meters({}, {})", c1, c2),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(dist > 10000.0 && dist < 200000.0, "distance: {}", dist);
}

#[test]
fn test_s2_parent() {
    let conn = setup_conn().unwrap();
    let cell: i64 = conn
        .query_row("SELECT s2_cell_id(39.9, 116.4, 10)", [], |row| row.get(0))
        .unwrap();
    let parent: i64 = conn
        .query_row(&format!("SELECT s2_parent({}, 8)", cell), [], |row| {
            row.get(0)
        })
        .unwrap();
    assert!(parent > 0 && parent != cell);
}

#[test]
fn test_st_transform_coords() {
    let conn = setup_conn().unwrap();
    let result: String = conn
        .query_row(
            "SELECT st_transform_coords(116.4, 39.9, 'EPSG:4326', 'EPSG:3857')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(result.starts_with("POINT"), "Got: {}", result);
}

#[test]
fn test_s2_area() {
    let conn = setup_conn().unwrap();
    let cell: i64 = conn
        .query_row("SELECT s2_cell_id(39.9, 116.4, 10)", [], |row| row.get(0))
        .unwrap();
    let area: f64 = conn
        .query_row(&format!("SELECT s2_area_m2({}, 10)", cell), [], |row| {
            row.get(0)
        })
        .unwrap();
    assert!(area > 0.0, "area: {}", area);
}

#[test]
fn test_s2_cell_level() {
    let conn = setup_conn().unwrap();
    let cell: i64 = conn
        .query_row("SELECT s2_cell_id(39.9, 116.4, 10)", [], |row| row.get(0))
        .unwrap();
    let level: i32 = conn
        .query_row(&format!("SELECT s2_cell_level({})", cell), [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(level, 10);
}

#[test]
fn test_s2_to_geo() {
    let conn = setup_conn().unwrap();
    let cell: i64 = conn
        .query_row("SELECT s2_cell_id(39.9, 116.4, 10)", [], |row| row.get(0))
        .unwrap();
    let geo: String = conn
        .query_row(&format!("SELECT s2_to_geo({})", cell), [], |row| row.get(0))
        .unwrap();
    assert!(geo.starts_with("POINT"));
}

#[test]
fn test_s2_cell_to_hex_roundtrip() {
    let conn = setup_conn().unwrap();
    let cell: i64 = conn
        .query_row("SELECT s2_cell_id(39.9, 116.4, 10)", [], |row| row.get(0))
        .unwrap();
    let hex: String = conn
        .query_row(&format!("SELECT s2_cell_to_hex({})", cell), [], |row| {
            row.get(0)
        })
        .unwrap();
    let back: i64 = conn
        .query_row(&format!("SELECT s2_hex_to_cell('{}')", hex), [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(cell, back);
}

#[test]
fn test_s2_covering() {
    let conn = setup_conn().unwrap();
    // Cover a small polygon in Beijing at level 8
    let count: usize = conn
        .query_row(
            "SELECT COUNT(*) FROM s2_covering('POLYGON((116.0 39.0, 117.0 39.0, 117.0 40.0, 116.0 40.0, 116.0 39.0))', 8, 10)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(count > 0, "covering should return cells");
}

#[test]
fn test_s2_children() {
    let conn = setup_conn().unwrap();
    let cell: i64 = conn
        .query_row("SELECT s2_cell_id(39.9, 116.4, 8)", [], |row| row.get(0))
        .unwrap();
    let count: usize = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM s2_children({}, 12)", cell),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        count, 256,
        "cell at level 8 has 4^(12-8)=256 children at level 12"
    );
}

#[test]
fn test_s2_cell_vertex() {
    let conn = setup_conn().unwrap();
    let cell: i64 = conn
        .query_row("SELECT s2_cell_id(39.9, 116.4, 10)", [], |row| row.get(0))
        .unwrap();
    let v: String = conn
        .query_row(&format!("SELECT s2_cell_vertex({}, 0)", cell), [], |row| {
            row.get(0)
        })
        .unwrap();
    assert!(v.starts_with("POINT"));
}

#[test]
fn test_s2_cell_id_from_point() {
    let conn = setup_conn().unwrap();
    let cell: i64 = conn
        .query_row(
            "SELECT s2_cell_id_from_point('POINT(116.4 39.9)')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(cell > 0);
}

#[test]
fn test_s2_cell_neighbors() {
    let conn = setup_conn().unwrap();
    let cell: i64 = conn
        .query_row("SELECT s2_cell_id(39.9, 116.4, 8)", [], |row| row.get(0))
        .unwrap();
    let count: usize = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM s2_cell_neighbors({})", cell),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 4, "each cell has 4 edge neighbors");
}

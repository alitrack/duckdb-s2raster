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
    conn.register_scalar_function::<duckdb_raster::StTransformCoords>("st_transform_coords")?;
    conn.register_scalar_function::<duckdb_raster::StTransform>("st_transform")?;
    conn.register_scalar_function::<duckdb_raster::RsValue>("rs_value")?;
    conn.register_table_function::<duckdb_raster::RsMetadataVTab>("rs_metadata")?;
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
        .query_row(
            &format!("SELECT s2_parent({}, 8)", cell),
            [],
            |row| row.get(0),
        )
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
        .query_row(
            &format!("SELECT s2_area_m2({}, 10)", cell),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(area > 0.0, "area: {}", area);
}

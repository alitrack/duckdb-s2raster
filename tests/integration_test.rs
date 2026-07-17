//! Integration tests for duckdb_raster extension.
//! Run with: cargo test --features test-bundled --test integration_test

use duckdb::Connection;

fn load_extension(conn: &Connection) {
    // In test-bundled mode, the extension is linked statically.
    // Extension functions are registered on connection open.
}

#[test]
fn test_s2_cell_id() {
    let conn = Connection::open_in_memory().unwrap();
    // Beijing: lat 39.9, lon 116.4
    let result: i64 = conn
        .query_row("SELECT s2_cell_id(39.9, 116.4, 10)", [], |row| row.get(0))
        .unwrap();
    assert!(result > 0, "Cell ID should be positive");
}

#[test]
fn test_s2_contains() {
    let conn = Connection::open_in_memory().unwrap();
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
    let conn = Connection::open_in_memory().unwrap();
    let c1: i64 = conn
        .query_row("SELECT s2_cell_id(39.9, 116.4, 10)", [], |row| row.get(0))
        .unwrap();
    let c2: i64 = conn
        .query_row("SELECT s2_cell_id(39.91, 116.41, 10)", [], |row| row.get(0))
        .unwrap();
    let dist: f64 = conn
        .query_row(
            &format!("SELECT s2_distance_meters({}, {})", c1, c2),
            [],
            |row| row.get(0),
        )
        .unwrap();
    // ~1.4 km at this lat/lon delta
    assert!(dist > 1000.0 && dist < 2000.0, "distance: {}", dist);
}

#[test]
fn test_s2_parent() {
    let conn = Connection::open_in_memory().unwrap();
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
    let conn = Connection::open_in_memory().unwrap();
    let result: String = conn
        .query_row(
            "SELECT st_transform_coords(116.4, 39.9, 'EPSG:4326', 'EPSG:3857')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(result.starts_with("POINT("), "Got: {}", result);
}

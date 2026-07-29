//! S2 Geometry implementation functions — pure, no DuckDB types.

use s2::cell::Cell;
use s2::cellid::CellID;
use s2::latlng::LatLng;
use s2::point::Point;

pub fn s2_cell_id_impl(lat: f64, lon: f64, level: i64) -> i64 {
    let ll = LatLng::from_degrees(lat, lon);
    let point = Point::from(ll);
    let cell = CellID::from(point);
    cell.parent(level as u64).0 as i64
}

pub fn s2_contains_impl(cell_id: i64, lat: f64, lon: f64) -> bool {
    let cell = CellID(cell_id as u64);
    let c: Cell = cell.into();
    let ll = LatLng::from_degrees(lat, lon);
    let point = Point::from(ll);
    c.contains_point(&point)
}

pub fn s2_distance_meters_impl(cell1: i64, cell2: i64) -> f64 {
    let c1: Cell = CellID(cell1 as u64).into();
    let c2: Cell = CellID(cell2 as u64).into();
    let p1 = c1.center();
    let p2 = c2.center();
    let dot = p1.0.dot(&p2.0);
    let angle = dot.min(1.0).max(-1.0).acos();
    angle * 6_371_009.0
}

pub fn s2_area_m2_impl(cell_id: i64, level: i32) -> f64 {
    let cid = CellID(cell_id as u64).parent(level as u64);
    let c: Cell = cid.into();
    c.approx_area() * 6_371_009.0_f64.powi(2)
}

pub fn s2_parent_impl(cell_id: i64, level: i64) -> i64 {
    CellID(cell_id as u64).parent(level as u64).0 as i64
}

pub fn s2_cell_level_impl(cell_id: i64) -> i32 {
    CellID(cell_id as u64).level() as i32
}

pub fn s2_to_geo_impl(cell_id: i64) -> String {
    let cell: Cell = CellID(cell_id as u64).into();
    let center = cell.center();
    let ll: LatLng = center.into();
    format!("POINT({:.7} {:.7})", ll.lng.deg(), ll.lat.deg())
}

pub fn s2_cell_to_hex_impl(cell_id: i64) -> String {
    CellID(cell_id as u64).to_token()
}

pub fn s2_hex_to_cell_impl(hex: &str) -> Option<i64> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        CellID::from_token(hex).0 as i64
    })).ok()
}

pub fn s2_cell_vertex_impl(cell_id: i64, k: i32) -> Option<String> {
    let cell: Cell = CellID(cell_id as u64).into();
    if k < 0 || k >= 4 { return None; }
    let v = cell.vertex(k as usize);
    let ll: LatLng = v.into();
    Some(format!("POINT({:.7} {:.7})", ll.lng.deg(), ll.lat.deg()))
}

pub fn s2_cell_id_from_point_impl(wkt_str: &str) -> i64 {
    use wkt::TryFromWkt;
    let geom = match geo::geometry::Geometry::<f64>::try_from_wkt_str(wkt_str) {
        Ok(g) => g,
        Err(_) => return 0,
    };
    let pt = match geom {
        geo::geometry::Geometry::Point(p) => p,
        _ => return 0,
    };
    let ll = LatLng::from_degrees(pt.y(), pt.x());
    let point = Point::from(ll);
    CellID::from(point).0 as i64
}

//! DuckDB Raster Extension — S2 geography + GeoTIFF raster
//! Pure Rust, zero system deps, DashMap concurrent cache.
//!
//! Functions: 34 scalar + 8 table = 42 total
//!   S2:         16 scalar + 4 table (covering, children, neighbors, interior_covering)
//!   Raster:     14 scalar + 4 table (rs_value, rs_metadata, rs_stats, rs_histogram)
//!   CellUnion:   4 scalar (pack, contains, intersects, area)

pub mod cell_union;
pub mod raster;
pub mod raster_scalar;
pub mod raster_table;
mod s2_impl;
pub mod s2_scalar;
pub mod s2_table;

#[cfg(feature = "loadable-extension")]
use duckdb::{Connection, Result};
#[cfg(feature = "loadable-extension")]
use std::error::Error;

#[cfg(feature = "loadable-extension")]
use duckdb::duckdb_entrypoint_c_api;

#[cfg(feature = "loadable-extension")]
#[duckdb_entrypoint_c_api(ext_name = "raster")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    // S2 scalar
    con.register_scalar_function::<s2_scalar::S2CellId>("s2_cell_id")?;
    con.register_scalar_function::<s2_scalar::S2Contains>("s2_contains")?;
    con.register_scalar_function::<s2_scalar::S2Distance>("s2_distance_meters")?;
    con.register_scalar_function::<s2_scalar::S2Area>("s2_area_m2")?;
    con.register_scalar_function::<s2_scalar::S2Parent>("s2_parent")?;
    con.register_scalar_function::<s2_scalar::S2CellLevel>("s2_cell_level")?;
    con.register_scalar_function::<s2_scalar::S2ToGeo>("s2_to_geo")?;
    con.register_scalar_function::<s2_scalar::S2CellToHex>("s2_cell_to_hex")?;
    con.register_scalar_function::<s2_scalar::S2HexToCell>("s2_hex_to_cell")?;
    con.register_scalar_function::<s2_scalar::S2CellVertex>("s2_cell_vertex")?;
    con.register_scalar_function::<s2_scalar::S2CellIdFromPoint>("s2_cell_id_from_point")?;

    // S2 table
    con.register_table_function::<s2_table::S2CoveringVTab>("s2_covering")?;
    con.register_table_function::<s2_table::S2ChildrenVTab>("s2_children")?;
    con.register_table_function::<s2_table::S2NeighborsVTab>("s2_cell_neighbors")?;
    con.register_table_function::<s2_table::S2InteriorCoveringVTab>("s2_interior_covering")?;

    // CellUnion
    con.register_scalar_function::<cell_union::S2CellUnionPack>("s2_cell_union_pack")?;
    con.register_scalar_function::<cell_union::S2CellUnionContains>("s2_cell_union_contains")?;
    con.register_scalar_function::<cell_union::S2CellUnionIntersects>("s2_cell_union_intersects")?;
    con.register_scalar_function::<cell_union::S2CellUnionArea>("s2_cell_union_area")?;

    // Raster scalar
    con.register_scalar_function::<raster_scalar::RsValue>("rs_value")?;
    con.register_scalar_function::<raster_scalar::RsBandCount>("rs_band_count")?;
    con.register_scalar_function::<raster_scalar::RsWidth>("rs_width")?;
    con.register_scalar_function::<raster_scalar::RsHeight>("rs_height")?;
    con.register_scalar_function::<raster_scalar::RsNodata>("rs_nodata")?;
    con.register_scalar_function::<raster_scalar::RsGeoTransform>("rs_geo_transform")?;
    con.register_scalar_function::<raster_scalar::RsPixelToWorld>("rs_pixel_to_world")?;
    con.register_scalar_function::<raster_scalar::RsWorldToPixel>("rs_world_to_pixel")?;
    con.register_scalar_function::<raster_scalar::RsScaleX>("rs_scale_x")?;
    con.register_scalar_function::<raster_scalar::RsScaleY>("rs_scale_y")?;
    con.register_scalar_function::<raster_scalar::RsCrs>("rs_crs")?;
    con.register_scalar_function::<raster_scalar::RsMin>("rs_min")?;
    con.register_scalar_function::<raster_scalar::RsMax>("rs_max")?;
    con.register_scalar_function::<raster_scalar::RsMean>("rs_mean")?;
    con.register_scalar_function::<raster_scalar::RsStddev>("rs_stddev")?;

    // Raster table
    con.register_table_function::<raster_table::RsMetadataVTab>("rs_metadata")?;
    con.register_table_function::<raster_table::RsStatsVTab>("rs_stats")?;
    con.register_table_function::<raster_table::RsHistogramVTab>("rs_histogram")?;

    Ok(())
}

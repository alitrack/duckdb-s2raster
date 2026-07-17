# duckdb-raster

DuckDB extension that ports SedonaDB's spatial algorithms: **S2 geography** + **ST_Transform (PROJ)** + **GeoTIFF raster** — all in pure Rust, zero system dependencies.

```sql
LOAD raster;

-- S2 geography: cell_id, containment, distance
SELECT s2_cell_id(39.9, 116.4, 10) AS cell;
SELECT s2_contains(cell, 39.91, 116.41) FROM ...;
SELECT s2_distance_meters(cell_a, cell_b) FROM ...;

-- S2 cell covering of a geometry
SELECT cell_id FROM s2_covering('POLYGON((...))', 8, 12);

-- Coordinate transformation via PROJ
SELECT st_transform_coords(116.4, 39.9, 'EPSG:4326', 'EPSG:3857');
SELECT st_transform(geom_wkt, 'EPSG:4326', 'EPSG:3857');

-- GeoTIFF raster (pure Rust, no GDAL needed)
SELECT * FROM rs_metadata('/path/to/dem.tif');
SELECT rs_value('/path/to/dem.tif', 1, 100, 200);
```

## Functions

### S2 Geography
| Function | Signature |
|----------|-----------|
| `s2_cell_id` | `(lat, lon, level) → cell_id` |
| `s2_contains` | `(cell_id, lat, lon) → bool` |
| `s2_distance_meters` | `(cell_id, cell_id) → meters` |
| `s2_area_m2` | `(cell_id, level) → m²` |
| `s2_parent` | `(cell_id, level) → parent_cell_id` |
| `s2_covering` | `(wkt, min_level, max_level) → table(cell_id)` |

### Coordinate Transformation
| Function | Signature |
|----------|-----------|
| `st_transform_coords` | `(x, y, from_crs, to_crs) → wkt_point` |
| `st_transform` | `(wkt, from_crs, to_crs) → wkt` |

### Raster (GeoTIFF)
| Function | Signature |
|----------|-----------|
| `rs_metadata` | `(path) → table(width, height, bands, crs, ...)` |
| `rs_value` | `(path, band, col, row) → pixel_value` |

## Install

Download from [Releases](https://github.com/alitrack/duckdb-raster/releases):

```sql
INSTALL raster FROM 'https://github.com/alitrack/duckdb-raster/releases/latest/download/raster.duckdb_extension';
LOAD raster;
```

## Build

```bash
# Requires Rust + cargo
git clone https://github.com/alitrack/duckdb-raster
cd duckdb-raster
cargo build --release

# Append DuckDB extension metadata
python3 metadata.py target/release/libduckdb_raster.so -o raster.duckdb_extension
```

## Why this exists

DuckDB's `spatial` extension covers basic vector operations but lacks:
- **S2 geography** — Google's S2 library for global-scale geo indexing (18 functions in SedonaDB)
- **ST_Transform** — CRS coordinate transformation via PROJ
- **Raster** — GeoTIFF reading, pixel access, basic raster operations (33 RS_* functions in SedonaDB)

This extension ports the algorithms from [Apache SedonaDB](https://github.com/apache/sedona-db) into a DuckDB-native format, using pure Rust crates (`s2`, `proj`, `tiff`) with zero system dependencies.

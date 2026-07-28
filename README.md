# duckdb-raster


**37 DuckDB SQL functions** (34 scalar UDFs + 8 table functions), 14 integration tests, file-level caching.

```sql
LOAD raster;

-- S2: cell_id, containment, distance, area, hex, neighbors, covering
SELECT s2_cell_id(39.9, 116.4, 10) AS cell;
SELECT s2_cell_neighbors(cell);
SELECT cell_id FROM s2_covering('POLYGON((...))', 8, 12);
SELECT cell_id FROM s2_interior_covering('POLYGON((...))', 8, 12, 20);

-- CellUnion: aggregate cells, query containment/intersection/area
WITH cu AS (
  SELECT s2_cell_union_pack(string_agg(s2_cell_to_hex(cid), ',')) AS blob
  FROM (SELECT s2_cell_id(lat, lon, 8) AS cid FROM points)
) SELECT s2_cell_union_area(blob), s2_cell_union_contains(blob, target);

SELECT st_transform(geom_wkt, 'EPSG:4326', 'EPSG:3857');

-- Raster: metadata, pixel access, stats, histograms
SELECT * FROM rs_metadata('/path/to/dem.tif');
SELECT * FROM rs_stats('/path/to/dem.tif', 1);
SELECT rs_value('/path/to/dem.tif', 1, 100, 200);
SELECT rs_pixel_to_world('/path/to/dem.tif', 100, 100);
```

## Functions (37 total)

### S2 Geography (20 functions — 18/18 SedonaDB parity)

| Function | Signature | Notes |
|----------|-----------|-------|
| `s2_cell_id` | `(lat, lon, level) → i64` | |
| `s2_contains` | `(cell_id, lat, lon) → bool` | |
| `s2_distance_meters` | `(cell_id, cell_id) → f64` | |
| `s2_area_m2` | `(cell_id, level) → f64` | |
| `s2_parent` | `(cell_id, level) → i64` | |
| `s2_cell_level` | `(cell_id) → i32` | |
| `s2_to_geo` | `(cell_id) → wkt` | Cell center as POINT |
| `s2_cell_to_hex` | `(cell_id) → varchar` | Token format |
| `s2_hex_to_cell` | `(hex) → i64` | Round-trip |
| `s2_cell_vertex` | `(cell_id, k) → wkt` | Vertex k (0-3) |
| `s2_cell_id_from_point` | `(wkt) → i64` | From WKT POINT |
| `s2_covering` | **table** `(wkt, min_lvl, max_lvl) → cell_id` | Bounding box |
| `s2_interior_covering` | **table** `(wkt, min, max, max_cells) → cell_id` | Strict interior |
| `s2_children` | **table** `(cell_id, level) → child_id` | |
| `s2_cell_neighbors` | **table** `(cell_id) → neighbor_id` | Edge neighbors |
| `s2_cell_union_pack` | `(csv_hex) → blob` | Build CellUnion |
| `s2_cell_union_contains` | `(blob, cell_id) → bool` | |
| `s2_cell_union_intersects` | `(blob, blob) → bool` | |
| `s2_cell_union_area` | `(blob) → f64` | m² |

- **Raster** — GeoTIFF reading, pixel access, statistics, histograms (pure Rust, zero GDAL deps)

This extension ports algorithms from [Apache SedonaDB](https://github.com/apache/sedona-db) into DuckDB-native format, using pure Rust crates (`s2`, `proj`, `tiff`, `geo`, `wkt`).

# ST_Distance Usage Guide

## Function Signature

```sql
ST_Distance(geometry1 TEXT, geometry2 TEXT) -> DOUBLE PRECISION
```

## Description

Calculates the Euclidean distance between two geometries specified in Well-Known Text (WKT) format.

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `geometry1` | TEXT | First geometry in WKT format |
| `geometry2` | TEXT | Second geometry in WKT format |

## Return Value

Returns a `DOUBLE PRECISION` value representing the minimum Euclidean distance between the two geometries. Returns `NULL` if either input is `NULL`.

## Supported Geometry Types

- **POINT**: `POINT(x y)`
- **LINESTRING**: `LINESTRING(x1 y1, x2 y2, ...)`
- **POLYGON**: `POLYGON((x1 y1, x2 y2, ..., x1 y1))`
- **MULTIPOINT**: `MULTIPOINT((x1 y1), (x2 y2), ...)`
- **MULTILINESTRING**: `MULTILINESTRING((x1 y1, x2 y2), (x3 y3, x4 y4))`
- **MULTIPOLYGON**: `MULTIPOLYGON(((x1 y1, ...)), ((x5 y5, ...)))`
- **GEOMETRYCOLLECTION**: Mixed geometry types

## Examples

### Example 1: Distance Between Two Points

```sql
SELECT ST_Distance('POINT(0 0)', 'POINT(3 4)') as distance;
```

**Result:**
```
 distance
----------
      5.0
```

**Explanation:** This is the classic 3-4-5 right triangle.

### Example 2: Distance From Point to LineString

```sql
SELECT ST_Distance(
    'POINT(5 5)',
    'LINESTRING(0 0, 10 0)'
) as distance;
```

**Result:**
```
 distance
----------
      5.0
```

**Explanation:** Perpendicular distance from point (5,5) to the line segment on the x-axis.

### Example 3: Distance From Point to Polygon

```sql
SELECT ST_Distance(
    'POINT(15 5)',
    'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'
) as distance;
```

**Result:**
```
 distance
----------
      5.0
```

**Explanation:** Distance from the point to the nearest edge of the square polygon.

### Example 4: Finding Nearby Locations

```sql
-- Find all stores within 5km of a given point
SELECT
    store_name,
    ST_Distance(location_wkt, 'POINT(-122.4194 37.7749)') as distance_km
FROM stores
WHERE ST_Distance(location_wkt, 'POINT(-122.4194 37.7749)') < 5.0
ORDER BY distance_km ASC
LIMIT 10;
```

### Example 5: Using in JOIN Conditions

```sql
-- Find pairs of cities that are close to each other
SELECT
    c1.name as city1,
    c2.name as city2,
    ST_Distance(c1.location, c2.location) as distance
FROM cities c1
JOIN cities c2
    ON ST_Distance(c1.location, c2.location) < 100
    AND c1.name < c2.name  -- Avoid duplicates
ORDER BY distance;
```

### Example 6: Handling NULL Values

```sql
SELECT
    ST_Distance('POINT(0 0)', NULL) as dist1,
    ST_Distance(NULL, 'POINT(1 1)') as dist2,
    ST_Distance(NULL, NULL) as dist3;
```

**Result:**
```
 dist1 | dist2 | dist3
-------+-------+-------
  NULL |  NULL |  NULL
```

### Example 7: Distance Matrix

```sql
-- Calculate distances between all pairs of points
WITH points AS (
    SELECT 'A' as name, 'POINT(0 0)' as geom
    UNION ALL
    SELECT 'B', 'POINT(3 0)'
    UNION ALL
    SELECT 'C', 'POINT(0 4)'
)
SELECT
    p1.name as from_point,
    p2.name as to_point,
    ST_Distance(p1.geom, p2.geom) as distance
FROM points p1
CROSS JOIN points p2
WHERE p1.name != p2.name;
```

### Example 8: Aggregating Distances

```sql
-- Calculate average distance from a central point to all stores
SELECT
    AVG(ST_Distance('POINT(-122.4194 37.7749)', location_wkt)) as avg_distance,
    MIN(ST_Distance('POINT(-122.4194 37.7749)', location_wkt)) as min_distance,
    MAX(ST_Distance('POINT(-122.4194 37.7749)', location_wkt)) as max_distance
FROM stores;
```

## Common Use Cases

### 1. Proximity Search
Find entities within a certain radius:
```sql
WHERE ST_Distance(location_wkt, 'POINT(x y)') < radius
```

### 2. Nearest Neighbor
Find the closest entity:
```sql
ORDER BY ST_Distance(location_wkt, 'POINT(x y)') ASC
LIMIT 1
```

### 3. Clustering Analysis
Group entities by proximity:
```sql
SELECT
    cluster_id,
    COUNT(*) as members,
    AVG(ST_Distance(location, centroid)) as avg_radius
FROM entities
GROUP BY cluster_id;
```

### 4. Route Optimization
Calculate total route distance:
```sql
SELECT
    SUM(ST_Distance(
        waypoint[i],
        waypoint[i+1]
    )) as total_distance
FROM routes;
```

## Performance Tips

### 1. Use Spatial Indexes
For large datasets, create a spatial index before running distance queries:
```sql
CREATE SPATIAL INDEX idx_locations ON stores(location_wkt);
```

### 2. Pre-filter with Bounding Boxes
Use bounding box checks before expensive distance calculations:
```sql
WHERE x BETWEEN min_x AND max_x
  AND y BETWEEN min_y AND max_y
  AND ST_Distance(location, target) < radius
```

### 3. Materialized Distance Columns
For frequently-queried distances, materialize them:
```sql
ALTER TABLE stores ADD COLUMN distance_to_center DOUBLE;
UPDATE stores SET distance_to_center = ST_Distance(location, 'POINT(0 0)');
```

### 4. Batch Processing
Process distance calculations in batches for better vectorization:
```sql
-- Good: Single query with multiple rows
SELECT id, ST_Distance(location, 'POINT(0 0)') FROM stores;

-- Less efficient: Multiple single-row queries
SELECT ST_Distance('POINT(x1 y1)', 'POINT(0 0)');
SELECT ST_Distance('POINT(x2 y2)', 'POINT(0 0)');
...
```

## Limitations

### Current Version (v1)
- **2D Only**: Only X and Y coordinates are considered (Z and M dimensions ignored)
- **Euclidean Distance**: Uses planar (flat-earth) distance calculations
- **No SRID**: Spatial Reference System ID not yet supported

### Coordinate System Considerations
- Assumes Cartesian coordinate system
- For geographic coordinates (lat/lon), results are in degrees, not meters
- Use ST_Distance_Sphere (future) for geographic distances

## Error Handling

### Invalid WKT Format
```sql
SELECT ST_Distance('POINT(0 0)', 'INVALID WKT');
-- ERROR: WKT parse error: unexpected token at position X
```

### Unclosed Geometries
```sql
SELECT ST_Distance('LINESTRING(0 0, 1 1', 'POINT(0 0)');
-- ERROR: WKT parse error: expected ')', found EOF
```

### Empty Geometries
```sql
SELECT ST_Distance('POINT EMPTY', 'POINT(0 0)');
-- ERROR: Failed to convert WKT to geo geometry
```

## Related Functions

- **ST_DWithin**: Check if geometries are within a distance (returns boolean)
- **ST_Distance_Sphere**: Geographic distance on a sphere (future)
- **ST_3DDistance**: 3D Euclidean distance (future)
- **ST_Length**: Calculate length of a linestring
- **ST_Area**: Calculate area of a polygon

## References

- PostGIS Documentation: https://postgis.net/docs/ST_Distance.html
- WKT Specification: https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry
- Geo Crate Documentation: https://docs.rs/geo/latest/geo/

## Version History

- **v1.0**: Initial implementation with WKT support and Euclidean distance
- **Future**: Geographic distances, 3D support, SRID handling

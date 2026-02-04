# Testing the different snapping methods

Merret, 2026-03-04

## Background

Our pygeoapi instance has a snapping process which allows to select the minimum
strahler order and then snap points to stream segments. This includes looking
for the nearest neighbours using the `<->` operator on PostGIS, to order the
potential stream segments by distance to the point to be snapped.

Previously, we ran that query in the pygeoapi processes on a geometry
column of the stream_segments table:

```
... ORDER BY seg.geom            <-> ST_SetSRID(ST_MakePoint(lon, lat), 4326)
```

But we figured out that using geometry will take lon, lat coordinates as plain
cartesian coordinates on a flat surface. It ignores the earth's round shape and the
resulting difference between one degree distance in north-south direction (constant)
and in east-west direction (gets smaller towards the poles due to the meridians
getting closer to each other, in a non-linear way). This difference is too big to
be neglected (in Berlin, about 53° North, this is already a difference of factor 2),
so selecting nearest neighbour based on that will lead to very wrong results.


We then tested converting the geometries on-the-fly to geographies:

```
... ORDER BY seg.geom::geography <-> ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography
```

But this is way to slow (see below).

So we will have to introduce a geography column in addition to the geometry column.
dev:

```
... ORDER BY seg.geog            <-> ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography
```

## Test cases:

* Plain geometry column (old way, entire table, wrong): Ca. 1 second for two points
* On-the-fly conversion to geography (super slow): Ca. 1500 seconds
* Pre-converted geography column (test table): Ca. 1 second for two points
* On-the-fly conversion to geography (small table): Ca 25 seconds for two points
* Pre-converted geography column (entire table): _To be done!_

To sum up:

The old way using the geometry is pretty fast (about 1 sec/2 points) - but wrong.

On-the-fly conversion to geography took ca. 1500 times longer (about 1494 sec/2 points, that almost 25 minutes)! Forget it.

Testing a preconverted geography column looks promising, it is pretty fast (about 1 sec/2 points). But we only tested that on one partition so far.

To compare a bit more realistically, we also ran the on-the-fly conversion on one partition, which took 25 times longer than with the pre-computed (about 25 sec/2 points).

Finally, we will have to compare the pre-converted geography column (for the entire table) with the on-the-fly converted geography (for the entire table) (the latter took about 1494 sec/2 points, that almost 25 minutes)...



### Plain geometry column (old way, wrong)

Using the old `ORDER BY seg.geom <-> ST_SetSRID(ST_MakePoint(lon, lat), 4326)` computation on plain geometry columns. This is reasonably fast, even on the entire table, but **wrong**.

```
query_nearest_with_geometry = '''
    UPDATE {tablename} AS temp1
    SET
        geom_closest = closest.geom,
        strahler_closest = closest.strahler,
        subcid_closest = closest.subc_id
    FROM {tablename} AS temp2
    CROSS JOIN LATERAL (
        SELECT seg.geom, seg.strahler, seg.subc_id
        FROM stream_segments seg
        WHERE seg.strahler >= {min_strahler}
        ORDER BY seg.geom <-> ST_SetSRID(ST_MakePoint(temp2.lon, temp2.lat), 4326)
        LIMIT 1
    ) AS closest
    WHERE temp1.geom_user = temp2.geom_user;
'''
```


### On-the-fly conversion to geography (super slow)

Converting the plain geometries to proper geographies on the fly, using `ORDER BY seg.geom::geography <-> ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography`. This is _EXTREMELY_ slow.

```
query_nearest_with_geography = '''
    UPDATE {tablename} AS temp1
    SET
        geom_closest = closest.geom,
        strahler_closest = closest.strahler,
        subcid_closest = closest.subc_id
    FROM {tablename} AS temp2
    CROSS JOIN LATERAL (
        SELECT seg.geom, seg.strahler, seg.subc_id
        FROM stream_segments seg
        WHERE seg.strahler >= {min_strahler}
        ORDER BY seg.geom::geography <-> ST_SetSRID(ST_MakePoint(temp2.lon, temp2.lat), 4326)::geography
        LIMIT 1
    ) AS closest
    WHERE temp1.geom_user = temp2.geom_user;
'''
```


### Pre-converted geography column (test table)

Vanessa generated one test table that contains a geography column (`"shiny_user"."stream_segments_geog_66"`). This covers only the region `66`, for testing.

```
query_nearest_with_geography_VB = '''
    UPDATE {tablename} AS temp1
    SET
        geom_closest = closest.geog,
        strahler_closest = closest.strahler,
        subcid_closest = closest.subc_id
    FROM {tablename} AS temp2
    CROSS JOIN LATERAL (
        SELECT seg.geog, seg.strahler, seg.subc_id
        FROM "shiny_user"."stream_segments_geog_66" seg
        WHERE seg.geog IS NOT NULL AND seg.strahler >= {min_strahler}
        ORDER BY seg.geog <-> ST_SetSRID(ST_MakePoint(temp2.lon, temp2.lat), 4326)::geography
        LIMIT 1
    ) AS closest
    WHERE temp1.geom_user = temp2.geom_user;
'''
```

### on-the-fly conversion to geography (small table)

Using the smaller table `"hydro.stream_segments66"`, to compare whether the speed-up of Vanessa's test table is (partially) due to using a smaller table. This partition covers only the region `66`, for comparison with the other test.

```
query_nearest_with_geography_66 = '''
    UPDATE {tablename} AS temp1
    SET
        geom_closest = closest.geom,
        strahler_closest = closest.strahler,
        subcid_closest = closest.subc_id
    FROM {tablename} AS temp2
    CROSS JOIN LATERAL (
        SELECT seg.geom, seg.strahler, seg.subc_id
        FROM hydro.stream_segments66 seg
        WHERE seg.strahler >= {min_strahler}
        ORDER BY seg.geom::geography <-> ST_SetSRID(ST_MakePoint(temp2.lon, temp2.lat), 4326)::geography
        LIMIT 1
    ) AS closest
    WHERE temp1.geom_user = temp2.geom_user;
'''
```

### Pre-converted geography column (entire table)

Once we will have added a `geography` type column to the entire table, we can test that.

```
query_nearest_with_geography_66 = '''
    UPDATE {tablename} AS temp1
    SET
        geom_closest = closest.geom,
        strahler_closest = closest.strahler,
        subcid_closest = closest.subc_id
    FROM {tablename} AS temp2
    CROSS JOIN LATERAL (
        SELECT seg.geom, seg.strahler, seg.subc_id
        FROM hydro.stream_segments seg
        WHERE seg.strahler >= {min_strahler}
        ORDER BY seg.geog <-> ST_SetSRID(ST_MakePoint(temp2.lon, temp2.lat), 4326)::geography
        LIMIT 1
    ) AS closest
    WHERE temp1.geom_user = temp2.geom_user;
'''
```

## Results


### Plain geometry column (old way, wrong)

```
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ python testscript_snapping_oldway_geometry.py
2026-02-04 14:31:44 - testscript:70 -  INFO - Testing query: query_nearest_with_geometry, starting at 20260204_14-31-44
2026-02-04 14:31:44 - testscript:89 - DEBUG - Connect to database...
2026-02-04 14:31:44 - database_connection:28 -  INFO - Database-Emergency-Off not configured (config file not found), using default (False).
2026-02-04 14:31:44 - temp_table_for_queries:105 - DEBUG - Preparing to insert data from a dataframe into PostGIS database...
2026-02-04 14:31:44 - temp_table_for_queries:138 - DEBUG - Created list of 2 insert rows...
2026-02-04 14:31:44 - temp_table_for_queries:139 - DEBUG - First insert row: ('FP1', 20.9890407160248, 40.2334685909601, ST_SetSRID(ST_MakePoint(20.9890407160248, 40.2334685909601), 4326))
2026-02-04 14:31:44 - temp_table_for_queries:149 - DEBUG - Creating and populating temp table "pygeo_49aacbe7b5ce4cacae5f56f1420e6fad"...
2026-02-04 14:31:44 - temp_table_for_queries:179 - DEBUG - Creating temporary table "pygeo_49aacbe7b5ce4cacae5f56f1420e6fad"...
2026-02-04 14:31:44 - temp_table_for_queries:200 - DEBUG - Creating temporary table "pygeo_49aacbe7b5ce4cacae5f56f1420e6fad"... done.
2026-02-04 14:31:44 - temp_table_for_queries:205 - DEBUG - Inserting into temporary table "pygeo_49aacbe7b5ce4cacae5f56f1420e6fad"...
2026-02-04 14:31:44 - temp_table_for_queries:216 - DEBUG - Inserting into temporary table "pygeo_49aacbe7b5ce4cacae5f56f1420e6fad"... done.
2026-02-04 14:31:44 - temp_table_for_queries:220 - DEBUG - Creating index for temporary table "pygeo_49aacbe7b5ce4cacae5f56f1420e6fad"...
2026-02-04 14:31:44 - temp_table_for_queries:230 - DEBUG - Creating index for temporary table "pygeo_49aacbe7b5ce4cacae5f56f1420e6fad"... done.
2026-02-04 14:31:44 - temp_table_for_queries:236 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_49aacbe7b5ce4cacae5f56f1420e6fad"...
2026-02-04 14:31:44 - temp_table_for_queries:254 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_49aacbe7b5ce4cacae5f56f1420e6fad"... done
2026-02-04 14:31:44 - temp_table_for_queries:265 - DEBUG - Set of distinct reg_ids present in the temp table: {66}
2026-02-04 14:31:44 - temp_table_for_queries:271 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_49aacbe7b5ce4cacae5f56f1420e6fad"...
2026-02-04 14:31:44 - temp_table_for_queries:289 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_49aacbe7b5ce4cacae5f56f1420e6fad"... done.
2026-02-04 14:31:44 - temp_table_for_queries:166 - DEBUG - Populating temp table "pygeo_49aacbe7b5ce4cacae5f56f1420e6fad" (incl. subc_id, basin_id, reg_id)... done.
2026-02-04 14:31:44 - testscript:122 -  INFO - Starting query: Nearest Neigbours
2026-02-04 14:31:45 - testscript:126 -  INFO - Finished query: Nearest Neigbours
2026-02-04 14:31:45 - testscript:127 - DEBUG - **** TIME ************: 0.24031758308410645
2026-02-04 14:31:45 - testscript:206 -  INFO - Starting query: Snapping without distance
2026-02-04 14:31:45 - testscript:210 -  INFO - Finished query: Snapping without distance
2026-02-04 14:31:45 - testscript:211 - DEBUG - **** TIME ************: 0.001992464065551758
2026-02-04 14:31:45 - testscript:218 - DEBUG - Making dataframe from database result...
2026-02-04 14:31:45 - snapping_strahler:548 - DEBUG - Generating dataframe to return...
2026-02-04 14:31:45 - testscript:220 - DEBUG - Storing dataframe to csv: test_query_nearest_with_geometry_20260204_14-31-44.csv...
2026-02-04 14:31:45 - temp_table_for_queries:31 - DEBUG - Dropping temporary table "pygeo_49aacbe7b5ce4cacae5f56f1420e6fad"...
2026-02-04 14:31:45 - temp_table_for_queries:34 - DEBUG - Dropping temporary table "pygeo_49aacbe7b5ce4cacae5f56f1420e6fad"... done.
2026-02-04 14:31:45 - testscript:226 -  INFO - Tested query: query_nearest_with_geometry, started  at 20260204_14-31-44
2026-02-04 14:31:45 - testscript:228 -  INFO - Tested query: query_nearest_with_geometry, finished at 20260204_14-31-45
2026-02-04 14:31:45 - testscript:229 -  INFO - Done.
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ 
```

### on-the-fly conversion to geography (small table)

```
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ python testscript_snapping_convert_to_geography_onthefly_subset66.py
2026-02-04 14:44:46 - testscript:70 -  INFO - Testing query: query_nearest_with_geography_66, starting at 20260204_14-44-46
2026-02-04 14:44:46 - testscript:89 - DEBUG - Connect to database...
2026-02-04 14:44:46 - database_connection:28 -  INFO - Database-Emergency-Off not configured (config file not found), using default (False).
2026-02-04 14:44:46 - temp_table_for_queries:105 - DEBUG - Preparing to insert data from a dataframe into PostGIS database...
2026-02-04 14:44:46 - temp_table_for_queries:138 - DEBUG - Created list of 2 insert rows...
2026-02-04 14:44:46 - temp_table_for_queries:139 - DEBUG - First insert row: ('FP1', 20.9890407160248, 40.2334685909601, ST_SetSRID(ST_MakePoint(20.9890407160248, 40.2334685909601), 4326))
2026-02-04 14:44:46 - temp_table_for_queries:149 - DEBUG - Creating and populating temp table "pygeo_1dbe3a1e0b7241fa94adf00b79cfd9db"...
2026-02-04 14:44:46 - temp_table_for_queries:179 - DEBUG - Creating temporary table "pygeo_1dbe3a1e0b7241fa94adf00b79cfd9db"...
2026-02-04 14:44:46 - temp_table_for_queries:200 - DEBUG - Creating temporary table "pygeo_1dbe3a1e0b7241fa94adf00b79cfd9db"... done.
2026-02-04 14:44:46 - temp_table_for_queries:205 - DEBUG - Inserting into temporary table "pygeo_1dbe3a1e0b7241fa94adf00b79cfd9db"...
2026-02-04 14:44:46 - temp_table_for_queries:216 - DEBUG - Inserting into temporary table "pygeo_1dbe3a1e0b7241fa94adf00b79cfd9db"... done.
2026-02-04 14:44:46 - temp_table_for_queries:220 - DEBUG - Creating index for temporary table "pygeo_1dbe3a1e0b7241fa94adf00b79cfd9db"...
2026-02-04 14:44:46 - temp_table_for_queries:230 - DEBUG - Creating index for temporary table "pygeo_1dbe3a1e0b7241fa94adf00b79cfd9db"... done.
2026-02-04 14:44:46 - temp_table_for_queries:236 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_1dbe3a1e0b7241fa94adf00b79cfd9db"...
2026-02-04 14:44:46 - temp_table_for_queries:254 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_1dbe3a1e0b7241fa94adf00b79cfd9db"... done
2026-02-04 14:44:46 - temp_table_for_queries:265 - DEBUG - Set of distinct reg_ids present in the temp table: {66}
2026-02-04 14:44:46 - temp_table_for_queries:271 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_1dbe3a1e0b7241fa94adf00b79cfd9db"...
2026-02-04 14:44:46 - temp_table_for_queries:289 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_1dbe3a1e0b7241fa94adf00b79cfd9db"... done.
2026-02-04 14:44:46 - temp_table_for_queries:166 - DEBUG - Populating temp table "pygeo_1dbe3a1e0b7241fa94adf00b79cfd9db" (incl. subc_id, basin_id, reg_id)... done.
2026-02-04 14:44:46 - testscript:122 -  INFO - Starting query: Nearest Neigbours
2026-02-04 14:45:10 - testscript:126 -  INFO - Finished query: Nearest Neigbours
2026-02-04 14:45:10 - testscript:127 - DEBUG - **** TIME ************: 24.354987382888794
2026-02-04 14:45:10 - testscript:206 -  INFO - Starting query: Snapping without distance
2026-02-04 14:45:10 - testscript:210 -  INFO - Finished query: Snapping without distance
2026-02-04 14:45:10 - testscript:211 - DEBUG - **** TIME ************: 0.00447845458984375
2026-02-04 14:45:10 - testscript:218 - DEBUG - Making dataframe from database result...
2026-02-04 14:45:10 - snapping_strahler:548 - DEBUG - Generating dataframe to return...
2026-02-04 14:45:11 - testscript:220 - DEBUG - Storing dataframe to csv: test_query_nearest_with_geography_66_20260204_14-44-46.csv...
2026-02-04 14:45:11 - temp_table_for_queries:31 - DEBUG - Dropping temporary table "pygeo_1dbe3a1e0b7241fa94adf00b79cfd9db"...
2026-02-04 14:45:11 - temp_table_for_queries:34 - DEBUG - Dropping temporary table "pygeo_1dbe3a1e0b7241fa94adf00b79cfd9db"... done.
2026-02-04 14:45:11 - testscript:226 -  INFO - Tested query: query_nearest_with_geography_66, started  at 20260204_14-44-46
2026-02-04 14:45:11 - testscript:228 -  INFO - Tested query: query_nearest_with_geography_66, finished at 20260204_14-45-11
2026-02-04 14:45:11 - testscript:229 -  INFO - Done.
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ 
```


### Pre-converted geography column (test table)


```
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ python testscript_snapping_preconverted_geography_subset66_vanessa.py
2026-02-04 14:35:07 - testscript:70 -  INFO - Testing query: query_nearest_with_geography_VB, starting at 20260204_14-35-07
2026-02-04 14:35:07 - testscript:89 - DEBUG - Connect to database...
2026-02-04 14:35:07 - database_connection:28 -  INFO - Database-Emergency-Off not configured (config file not found), using default (False).
2026-02-04 14:35:07 - temp_table_for_queries:105 - DEBUG - Preparing to insert data from a dataframe into PostGIS database...
2026-02-04 14:35:07 - temp_table_for_queries:138 - DEBUG - Created list of 2 insert rows...
2026-02-04 14:35:07 - temp_table_for_queries:139 - DEBUG - First insert row: ('FP1', 20.9890407160248, 40.2334685909601, ST_SetSRID(ST_MakePoint(20.9890407160248, 40.2334685909601), 4326))
2026-02-04 14:35:07 - temp_table_for_queries:149 - DEBUG - Creating and populating temp table "pygeo_a91d3af3181544179de2cf956433a0cc"...
2026-02-04 14:35:07 - temp_table_for_queries:179 - DEBUG - Creating temporary table "pygeo_a91d3af3181544179de2cf956433a0cc"...
2026-02-04 14:35:07 - temp_table_for_queries:200 - DEBUG - Creating temporary table "pygeo_a91d3af3181544179de2cf956433a0cc"... done.
2026-02-04 14:35:07 - temp_table_for_queries:205 - DEBUG - Inserting into temporary table "pygeo_a91d3af3181544179de2cf956433a0cc"...
2026-02-04 14:35:07 - temp_table_for_queries:216 - DEBUG - Inserting into temporary table "pygeo_a91d3af3181544179de2cf956433a0cc"... done.
2026-02-04 14:35:07 - temp_table_for_queries:220 - DEBUG - Creating index for temporary table "pygeo_a91d3af3181544179de2cf956433a0cc"...
2026-02-04 14:35:07 - temp_table_for_queries:230 - DEBUG - Creating index for temporary table "pygeo_a91d3af3181544179de2cf956433a0cc"... done.
2026-02-04 14:35:07 - temp_table_for_queries:236 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_a91d3af3181544179de2cf956433a0cc"...
2026-02-04 14:35:08 - temp_table_for_queries:254 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_a91d3af3181544179de2cf956433a0cc"... done
2026-02-04 14:35:08 - temp_table_for_queries:265 - DEBUG - Set of distinct reg_ids present in the temp table: {66}
2026-02-04 14:35:08 - temp_table_for_queries:271 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_a91d3af3181544179de2cf956433a0cc"...
2026-02-04 14:35:08 - temp_table_for_queries:289 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_a91d3af3181544179de2cf956433a0cc"... done.
2026-02-04 14:35:08 - temp_table_for_queries:166 - DEBUG - Populating temp table "pygeo_a91d3af3181544179de2cf956433a0cc" (incl. subc_id, basin_id, reg_id)... done.
2026-02-04 14:35:08 - testscript:122 -  INFO - Starting query: Nearest Neigbours
2026-02-04 14:35:08 - testscript:126 -  INFO - Finished query: Nearest Neigbours
2026-02-04 14:35:08 - testscript:127 - DEBUG - **** TIME ************: 0.016571044921875
2026-02-04 14:35:08 - testscript:206 -  INFO - Starting query: Snapping without distance
2026-02-04 14:35:08 - testscript:210 -  INFO - Finished query: Snapping without distance
2026-02-04 14:35:08 - testscript:211 - DEBUG - **** TIME ************: 0.0011515617370605469
2026-02-04 14:35:08 - testscript:218 - DEBUG - Making dataframe from database result...
2026-02-04 14:35:08 - snapping_strahler:548 - DEBUG - Generating dataframe to return...
2026-02-04 14:35:08 - testscript:220 - DEBUG - Storing dataframe to csv: test_query_nearest_with_geography_VB_20260204_14-35-07.csv...
2026-02-04 14:35:08 - temp_table_for_queries:31 - DEBUG - Dropping temporary table "pygeo_a91d3af3181544179de2cf956433a0cc"...
2026-02-04 14:35:08 - temp_table_for_queries:34 - DEBUG - Dropping temporary table "pygeo_a91d3af3181544179de2cf956433a0cc"... done.
2026-02-04 14:35:08 - testscript:226 -  INFO - Tested query: query_nearest_with_geography_VB, started  at 20260204_14-35-07
2026-02-04 14:35:08 - testscript:228 -  INFO - Tested query: query_nearest_with_geography_VB, finished at 20260204_14-35-08
2026-02-04 14:35:08 - testscript:229 -  INFO - Done.
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ 
```

### On-the-fly conversion to geography (super slow)

```
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ python testscript_snapping_convert_to_geography_onthefly.py
2026-02-04 14:46:36 - testscript:70 -  INFO - Testing query: query_nearest_with_geography, starting at 20260204_14-46-36
2026-02-04 14:46:36 - testscript:89 - DEBUG - Connect to database...
2026-02-04 14:46:36 - database_connection:28 -  INFO - Database-Emergency-Off not configured (config file not found), using default (False).
2026-02-04 14:46:36 - temp_table_for_queries:105 - DEBUG - Preparing to insert data from a dataframe into PostGIS database...
2026-02-04 14:46:36 - temp_table_for_queries:138 - DEBUG - Created list of 2 insert rows...
2026-02-04 14:46:36 - temp_table_for_queries:139 - DEBUG - First insert row: ('FP1', 20.9890407160248, 40.2334685909601, ST_SetSRID(ST_MakePoint(20.9890407160248, 40.2334685909601), 4326))
2026-02-04 14:46:36 - temp_table_for_queries:149 - DEBUG - Creating and populating temp table "pygeo_a946a77b81834d3ab831bccd503d0c29"...
2026-02-04 14:46:36 - temp_table_for_queries:179 - DEBUG - Creating temporary table "pygeo_a946a77b81834d3ab831bccd503d0c29"...
2026-02-04 14:46:36 - temp_table_for_queries:200 - DEBUG - Creating temporary table "pygeo_a946a77b81834d3ab831bccd503d0c29"... done.
2026-02-04 14:46:36 - temp_table_for_queries:205 - DEBUG - Inserting into temporary table "pygeo_a946a77b81834d3ab831bccd503d0c29"...
2026-02-04 14:46:36 - temp_table_for_queries:216 - DEBUG - Inserting into temporary table "pygeo_a946a77b81834d3ab831bccd503d0c29"... done.
2026-02-04 14:46:36 - temp_table_for_queries:220 - DEBUG - Creating index for temporary table "pygeo_a946a77b81834d3ab831bccd503d0c29"...
2026-02-04 14:46:36 - temp_table_for_queries:230 - DEBUG - Creating index for temporary table "pygeo_a946a77b81834d3ab831bccd503d0c29"... done.
2026-02-04 14:46:36 - temp_table_for_queries:236 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_a946a77b81834d3ab831bccd503d0c29"...
2026-02-04 14:46:36 - temp_table_for_queries:254 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_a946a77b81834d3ab831bccd503d0c29"... done
2026-02-04 14:46:36 - temp_table_for_queries:265 - DEBUG - Set of distinct reg_ids present in the temp table: {66}
2026-02-04 14:46:36 - temp_table_for_queries:271 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_a946a77b81834d3ab831bccd503d0c29"...
2026-02-04 14:46:36 - temp_table_for_queries:289 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_a946a77b81834d3ab831bccd503d0c29"... done.
2026-02-04 14:46:36 - temp_table_for_queries:166 - DEBUG - Populating temp table "pygeo_a946a77b81834d3ab831bccd503d0c29" (incl. subc_id, basin_id, reg_id)... done.
2026-02-04 14:46:36 - testscript:122 -  INFO - Starting query: Nearest Neigbours
2026-02-04 15:11:30 - testscript:126 -  INFO - Finished query: Nearest Neigbours
2026-02-04 15:11:30 - testscript:127 - DEBUG - **** TIME ************: 1494.2884843349457
2026-02-04 15:11:30 - testscript:206 -  INFO - Starting query: Snapping without distance
2026-02-04 15:11:30 - testscript:210 -  INFO - Finished query: Snapping without distance
2026-02-04 15:11:30 - testscript:211 - DEBUG - **** TIME ************: 0.008788824081420898
2026-02-04 15:11:30 - testscript:218 - DEBUG - Making dataframe from database result...
2026-02-04 15:11:30 - snapping_strahler:548 - DEBUG - Generating dataframe to return...
2026-02-04 15:11:30 - testscript:220 - DEBUG - Storing dataframe to csv: test_query_nearest_with_geography_20260204_14-46-36.csv...
2026-02-04 15:11:30 - temp_table_for_queries:31 - DEBUG - Dropping temporary table "pygeo_a946a77b81834d3ab831bccd503d0c29"...
2026-02-04 15:11:30 - temp_table_for_queries:34 - DEBUG - Dropping temporary table "pygeo_a946a77b81834d3ab831bccd503d0c29"... done.
2026-02-04 15:11:30 - testscript:226 -  INFO - Tested query: query_nearest_with_geography, started  at 20260204_14-46-36
2026-02-04 15:11:30 - testscript:228 -  INFO - Tested query: query_nearest_with_geography, finished at 20260204_15-11-30
2026-02-04 15:11:30 - testscript:229 -  INFO - Done.
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ 
```

This took approximately 24 minutes 54 seconds, or 1494 seconds, ca. 1500 times more than on a geometry column.

### Pre-converted geography column (entire table)

Once we will have added a `geography` type column to the entire table, we can test that.


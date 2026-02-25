# Testing the different snapping methods

Merret, 2026-02-04

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

For the distance computation between the original point and the snapped point (using
`ST_Distance()`), we also have to use geographies instead of geometries.

And it is questionable whether we should also use geographies for the actual snapping
step, which uses `ST_LineLocatePoint()` and `ST_LineInterpolatePoint()`.

So these options exist:

* Old: Using geometries for all three steps. Plain wrong. Fast, as we have geometry columns.
* Type-casting on the fly: Using geographies, but type-casting them on-the-fly, as we don't
  have geography columns yet. Super slow, of course.
* Pre-converted geography columns: Using geographies, from geography columns. For this, we
  only have one table (one partition) with a pre-converted geography column, for testing.

So for valid comparison, we also run the type-casting way on just one partition. As the fact
of running it on one small partition compared to running it on the entire table might be part
of the speed-up.


## Snapping step also on geographies

Another aspect, added later: We use a temporary table to store the points that should be
snapped, and to compute the distance etc. This temp table could also be either in geometry
(so far), or directly in geography, or type-casting.

In the latter two cases, the snapping step and the distance computation step are then run
on geographies, no longer on geometries.

I added tests on that a week later.

### Type casting to geog, for snapping step

So I added a test where the temp table is still in geometry, but instead of type-casting
the linestring to geometry for the snapping step (so both are geometry), we type-cast
the point to a geography for the snapping step (so both are geography).

```
# test script:
testscript_snapping_preconverted_geography_subset66_vanessa_cast_to_geog.py

# NOT:
ST_LineLocatePoint(temp.geom_closest::geometry, temp.geom_user)

# BUT:
ST_LineLocatePoint(temp.geom_closest::geography, temp.geom_user::geography)
# OR: 
ST_LineLocatePoint(temp.geom_closest::geography(LINESTRING, 4326), temp.geom_user::geography(POINT, 4326)) 
```

Unfortunately, this ran into errors, as our PostGIS version is too old
to support `ST_LineLocatePoint(geography, geography)`.

### Temp table in geography type

In this test, I defined new functions to create the temp table, and defined the columns
as geography columns right away:

```
testscript_snapping_preconverted_geography_subset66_vanessa_geogtemp.py

# But this snapping test fails, as our PostGIS version is too old
# to support ST_LineLocatePoint(geography, geography):
psycopg2.errors.UndefinedFunction: function st_linelocatepoint(geography, geometry) does not exist
LINE 5:                 ST_LineLocatePoint(temp.geog_closest, temp.g...
                        ^
```


## Test cases:

* Plain geometry column (old way, entire table, wrong): Ca. 1 second for two points
* On-the-fly conversion to geography (just one partition, slow): Ca 25 seconds for two points
* On-the-fly conversion to geography (entire table, super slow): Ca. 1500 seconds
* Pre-converted geography column (just one partition): Ca. 1 second for two points
* Pre-converted geography column (entire table): _To be done!_

To sum up:

* The old way using the geometry is pretty fast (about 1 sec/2 points) - but wrong.
* On-the-fly conversion to geography took ca. 1500 times longer (about 1494 sec/2 points, that almost 25 minutes)! Forget it.
* Testing a preconverted geography column looks promising, it is pretty fast (about 1 sec/2 points). But we only tested that on one partition so far.
* To compare a bit more realistically, we also ran the on-the-fly conversion on one partition, which took 25 times longer than with the pre-computed (about 25 sec/2 points).
* Finally, we will have to compare the pre-converted geography column (for the entire table) with the on-the-fly converted geography (for the entire table) (the latter took about 1494 sec/2 points, that almost 25 minutes)...

## How to run

```
# clone repos
git clone git@github.com:glowabio/aqua90m.git
cd aqua90m

# create virtual env and install dependencies
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

# make sure you have config:
cat /opt/pyg_upstream_dev/pygeoapi/config.geofreshprod.json

# run:
cp geofresh/tests_snapping_methods_20260204/*.py geofresh/
cd geofresh
python testscript_snapping_preconverted_geography_subset66_vanessa.py
# note: To use input csv files, add the path or URL in: csv_url_or_path
# also change min_strahler, if desired
# note: distance computation was not tested yet.
```

Results will be written as csv into the same directory.

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


## Results for 23 points

### Plain geometry column (old way, wrong)

```
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ python testscript_snapping_oldway_geometry.py 
2026-02-04 15:56:20 - testscript:70 -  INFO - Testing query: query_nearest_with_geometry, starting at 20260204_15-56-20
2026-02-04 15:56:20 - testscript:80 - DEBUG - Reading CSV: /var/www/nginx/referencedata/aqua90m/spdata_barbus_with_basinid.csv
2026-02-04 15:56:20 - testscript:89 - DEBUG - Connect to database...
2026-02-04 15:56:20 - database_connection:28 -  INFO - Database-Emergency-Off not configured (config file not found), using default (False).
2026-02-04 15:56:21 - temp_table_for_queries:105 - DEBUG - Preparing to insert data from a dataframe into PostGIS database...
2026-02-04 15:56:21 - temp_table_for_queries:138 - DEBUG - Created list of 23 insert rows...
2026-02-04 15:56:21 - temp_table_for_queries:139 - DEBUG - First insert row: ('FP1', 20.9890407160248, 40.2334685909601, ST_SetSRID(ST_MakePoint(20.9890407160248, 40.2334685909601), 4326))
2026-02-04 15:56:21 - temp_table_for_queries:149 - DEBUG - Creating and populating temp table "pygeo_3eb7e7fb90604d2498d1bcbb6c970f4c"...
2026-02-04 15:56:21 - temp_table_for_queries:179 - DEBUG - Creating temporary table "pygeo_3eb7e7fb90604d2498d1bcbb6c970f4c"...
2026-02-04 15:56:21 - temp_table_for_queries:200 - DEBUG - Creating temporary table "pygeo_3eb7e7fb90604d2498d1bcbb6c970f4c"... done.
2026-02-04 15:56:21 - temp_table_for_queries:205 - DEBUG - Inserting into temporary table "pygeo_3eb7e7fb90604d2498d1bcbb6c970f4c"...
2026-02-04 15:56:21 - temp_table_for_queries:216 - DEBUG - Inserting into temporary table "pygeo_3eb7e7fb90604d2498d1bcbb6c970f4c"... done.
2026-02-04 15:56:21 - temp_table_for_queries:220 - DEBUG - Creating index for temporary table "pygeo_3eb7e7fb90604d2498d1bcbb6c970f4c"...
2026-02-04 15:56:21 - temp_table_for_queries:230 - DEBUG - Creating index for temporary table "pygeo_3eb7e7fb90604d2498d1bcbb6c970f4c"... done.
2026-02-04 15:56:21 - temp_table_for_queries:236 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_3eb7e7fb90604d2498d1bcbb6c970f4c"...
2026-02-04 15:56:21 - temp_table_for_queries:254 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_3eb7e7fb90604d2498d1bcbb6c970f4c"... done
2026-02-04 15:56:21 - temp_table_for_queries:265 - DEBUG - Set of distinct reg_ids present in the temp table: {66}
2026-02-04 15:56:21 - temp_table_for_queries:271 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_3eb7e7fb90604d2498d1bcbb6c970f4c"...
2026-02-04 15:56:21 - temp_table_for_queries:289 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_3eb7e7fb90604d2498d1bcbb6c970f4c"... done.
2026-02-04 15:56:21 - temp_table_for_queries:166 - DEBUG - Populating temp table "pygeo_3eb7e7fb90604d2498d1bcbb6c970f4c" (incl. subc_id, basin_id, reg_id)... done.
2026-02-04 15:56:21 - testscript:122 -  INFO - Starting query: Nearest Neigbours
2026-02-04 15:56:23 - testscript:126 -  INFO - Finished query: Nearest Neigbours
2026-02-04 15:56:23 - testscript:127 - DEBUG - **** TIME ************: 1.5304055213928223
2026-02-04 15:56:23 - testscript:206 -  INFO - Starting query: Snapping without distance
2026-02-04 15:56:23 - testscript:210 -  INFO - Finished query: Snapping without distance
2026-02-04 15:56:23 - testscript:211 - DEBUG - **** TIME ************: 0.0023691654205322266
2026-02-04 15:56:23 - testscript:218 - DEBUG - Making dataframe from database result...
2026-02-04 15:56:23 - snapping_strahler:548 - DEBUG - Generating dataframe to return...
2026-02-04 15:56:23 - testscript:220 - DEBUG - Storing dataframe to csv: test_query_nearest_with_geometry_20260204_15-56-20.csv...
2026-02-04 15:56:23 - temp_table_for_queries:31 - DEBUG - Dropping temporary table "pygeo_3eb7e7fb90604d2498d1bcbb6c970f4c"...
2026-02-04 15:56:23 - temp_table_for_queries:34 - DEBUG - Dropping temporary table "pygeo_3eb7e7fb90604d2498d1bcbb6c970f4c"... done.
2026-02-04 15:56:23 - testscript:226 -  INFO - Tested query: query_nearest_with_geometry, started  at 20260204_15-56-20
2026-02-04 15:56:23 - testscript:228 -  INFO - Tested query: query_nearest_with_geometry, finished at 20260204_15-56-23
2026-02-04 15:56:23 - testscript:229 -  INFO - Done.
```

### On-the-fly conversion to geography (small table)

```
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ python testscript_snapping_convert_to_geography_onthefly_subset66.py
2026-02-04 15:59:13 - testscript:70 -  INFO - Testing query: query_nearest_with_geography_66, starting at 20260204_15-59-13
2026-02-04 15:59:13 - testscript:80 - DEBUG - Reading CSV: /var/www/nginx/referencedata/aqua90m/spdata_barbus_with_basinid.csv
2026-02-04 15:59:13 - testscript:89 - DEBUG - Connect to database...
2026-02-04 15:59:13 - database_connection:28 -  INFO - Database-Emergency-Off not configured (config file not found), using default (False).
2026-02-04 15:59:13 - temp_table_for_queries:105 - DEBUG - Preparing to insert data from a dataframe into PostGIS database...
2026-02-04 15:59:13 - temp_table_for_queries:138 - DEBUG - Created list of 23 insert rows...
2026-02-04 15:59:13 - temp_table_for_queries:139 - DEBUG - First insert row: ('FP1', 20.9890407160248, 40.2334685909601, ST_SetSRID(ST_MakePoint(20.9890407160248, 40.2334685909601), 4326))
2026-02-04 15:59:13 - temp_table_for_queries:149 - DEBUG - Creating and populating temp table "pygeo_dc444f3e4172433d8443874dd329ea0e"...
2026-02-04 15:59:13 - temp_table_for_queries:179 - DEBUG - Creating temporary table "pygeo_dc444f3e4172433d8443874dd329ea0e"...
2026-02-04 15:59:13 - temp_table_for_queries:200 - DEBUG - Creating temporary table "pygeo_dc444f3e4172433d8443874dd329ea0e"... done.
2026-02-04 15:59:13 - temp_table_for_queries:205 - DEBUG - Inserting into temporary table "pygeo_dc444f3e4172433d8443874dd329ea0e"...
2026-02-04 15:59:13 - temp_table_for_queries:216 - DEBUG - Inserting into temporary table "pygeo_dc444f3e4172433d8443874dd329ea0e"... done.
2026-02-04 15:59:13 - temp_table_for_queries:220 - DEBUG - Creating index for temporary table "pygeo_dc444f3e4172433d8443874dd329ea0e"...
2026-02-04 15:59:13 - temp_table_for_queries:230 - DEBUG - Creating index for temporary table "pygeo_dc444f3e4172433d8443874dd329ea0e"... done.
2026-02-04 15:59:13 - temp_table_for_queries:236 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_dc444f3e4172433d8443874dd329ea0e"...
2026-02-04 15:59:13 - temp_table_for_queries:254 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_dc444f3e4172433d8443874dd329ea0e"... done
2026-02-04 15:59:13 - temp_table_for_queries:265 - DEBUG - Set of distinct reg_ids present in the temp table: {66}
2026-02-04 15:59:13 - temp_table_for_queries:271 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_dc444f3e4172433d8443874dd329ea0e"...
2026-02-04 15:59:13 - temp_table_for_queries:289 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_dc444f3e4172433d8443874dd329ea0e"... done.
2026-02-04 15:59:13 - temp_table_for_queries:166 - DEBUG - Populating temp table "pygeo_dc444f3e4172433d8443874dd329ea0e" (incl. subc_id, basin_id, reg_id)... done.
2026-02-04 15:59:13 - testscript:122 -  INFO - Starting query: Nearest Neigbours
2026-02-04 16:01:08 - testscript:126 -  INFO - Finished query: Nearest Neigbours
2026-02-04 16:01:08 - testscript:127 - DEBUG - **** TIME ************: 114.31312131881714
2026-02-04 16:01:08 - testscript:206 -  INFO - Starting query: Snapping without distance
2026-02-04 16:01:08 - testscript:210 -  INFO - Finished query: Snapping without distance
2026-02-04 16:01:08 - testscript:211 - DEBUG - **** TIME ************: 0.0018162727355957031
2026-02-04 16:01:08 - testscript:218 - DEBUG - Making dataframe from database result...
2026-02-04 16:01:08 - snapping_strahler:548 - DEBUG - Generating dataframe to return...
2026-02-04 16:01:08 - testscript:220 - DEBUG - Storing dataframe to csv: test_query_nearest_with_geography_66_20260204_15-59-13.csv...
2026-02-04 16:01:08 - temp_table_for_queries:31 - DEBUG - Dropping temporary table "pygeo_dc444f3e4172433d8443874dd329ea0e"...
2026-02-04 16:01:08 - temp_table_for_queries:34 - DEBUG - Dropping temporary table "pygeo_dc444f3e4172433d8443874dd329ea0e"... done.
2026-02-04 16:01:08 - testscript:226 -  INFO - Tested query: query_nearest_with_geography_66, started  at 20260204_15-59-13
2026-02-04 16:01:08 - testscript:228 -  INFO - Tested query: query_nearest_with_geography_66, finished at 20260204_16-01-08
2026-02-04 16:01:08 - testscript:229 -  INFO - Done.
```

### Pre-converted geography column (test table)

```
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ python testscript_snapping_preconverted_geography_subset66_vanessa.py
2026-02-04 15:58:21 - testscript:70 -  INFO - Testing query: query_nearest_with_geography_VB, starting at 20260204_15-58-21
2026-02-04 15:58:21 - testscript:80 - DEBUG - Reading CSV: /var/www/nginx/referencedata/aqua90m/spdata_barbus_with_basinid.csv
2026-02-04 15:58:21 - testscript:89 - DEBUG - Connect to database...
2026-02-04 15:58:21 - database_connection:28 -  INFO - Database-Emergency-Off not configured (config file not found), using default (False).
2026-02-04 15:58:21 - temp_table_for_queries:105 - DEBUG - Preparing to insert data from a dataframe into PostGIS database...
2026-02-04 15:58:21 - temp_table_for_queries:138 - DEBUG - Created list of 23 insert rows...
2026-02-04 15:58:21 - temp_table_for_queries:139 - DEBUG - First insert row: ('FP1', 20.9890407160248, 40.2334685909601, ST_SetSRID(ST_MakePoint(20.9890407160248, 40.2334685909601), 4326))
2026-02-04 15:58:21 - temp_table_for_queries:149 - DEBUG - Creating and populating temp table "pygeo_00ace9d8f155489aaebc621be8a8f70f"...
2026-02-04 15:58:21 - temp_table_for_queries:179 - DEBUG - Creating temporary table "pygeo_00ace9d8f155489aaebc621be8a8f70f"...
2026-02-04 15:58:21 - temp_table_for_queries:200 - DEBUG - Creating temporary table "pygeo_00ace9d8f155489aaebc621be8a8f70f"... done.
2026-02-04 15:58:21 - temp_table_for_queries:205 - DEBUG - Inserting into temporary table "pygeo_00ace9d8f155489aaebc621be8a8f70f"...
2026-02-04 15:58:21 - temp_table_for_queries:216 - DEBUG - Inserting into temporary table "pygeo_00ace9d8f155489aaebc621be8a8f70f"... done.
2026-02-04 15:58:21 - temp_table_for_queries:220 - DEBUG - Creating index for temporary table "pygeo_00ace9d8f155489aaebc621be8a8f70f"...
2026-02-04 15:58:21 - temp_table_for_queries:230 - DEBUG - Creating index for temporary table "pygeo_00ace9d8f155489aaebc621be8a8f70f"... done.
2026-02-04 15:58:21 - temp_table_for_queries:236 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_00ace9d8f155489aaebc621be8a8f70f"...
2026-02-04 15:58:21 - temp_table_for_queries:254 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_00ace9d8f155489aaebc621be8a8f70f"... done
2026-02-04 15:58:21 - temp_table_for_queries:265 - DEBUG - Set of distinct reg_ids present in the temp table: {66}
2026-02-04 15:58:21 - temp_table_for_queries:271 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_00ace9d8f155489aaebc621be8a8f70f"...
2026-02-04 15:58:22 - temp_table_for_queries:289 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_00ace9d8f155489aaebc621be8a8f70f"... done.
2026-02-04 15:58:22 - temp_table_for_queries:166 - DEBUG - Populating temp table "pygeo_00ace9d8f155489aaebc621be8a8f70f" (incl. subc_id, basin_id, reg_id)... done.
2026-02-04 15:58:22 - testscript:122 -  INFO - Starting query: Nearest Neigbours
2026-02-04 15:58:22 - testscript:126 -  INFO - Finished query: Nearest Neigbours
2026-02-04 15:58:22 - testscript:127 - DEBUG - **** TIME ************: 0.7690660953521729
2026-02-04 15:58:22 - testscript:206 -  INFO - Starting query: Snapping without distance
2026-02-04 15:58:22 - testscript:210 -  INFO - Finished query: Snapping without distance
2026-02-04 15:58:22 - testscript:211 - DEBUG - **** TIME ************: 0.0029947757720947266
2026-02-04 15:58:22 - testscript:218 - DEBUG - Making dataframe from database result...
2026-02-04 15:58:22 - snapping_strahler:548 - DEBUG - Generating dataframe to return...
2026-02-04 15:58:22 - testscript:220 - DEBUG - Storing dataframe to csv: test_query_nearest_with_geography_VB_20260204_15-58-21.csv...
2026-02-04 15:58:22 - temp_table_for_queries:31 - DEBUG - Dropping temporary table "pygeo_00ace9d8f155489aaebc621be8a8f70f"...
2026-02-04 15:58:22 - temp_table_for_queries:34 - DEBUG - Dropping temporary table "pygeo_00ace9d8f155489aaebc621be8a8f70f"... done.
2026-02-04 15:58:22 - testscript:226 -  INFO - Tested query: query_nearest_with_geography_VB, started  at 20260204_15-58-21
2026-02-04 15:58:22 - testscript:228 -  INFO - Tested query: query_nearest_with_geography_VB, finished at 20260204_15-58-22
2026-02-04 15:58:22 - testscript:229 -  INFO - Done.
```

### Pre-converted geography column (entire table)

Does not exist yet

### On-the-fly conversion to geography (super slow)

We're not even trying this...



## Results for 3624 points

### Plain geometry column (old way, wrong)

```
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ python testscript_snapping_oldway_geometry.py
2026-02-05 15:43:52 - testscript:74 -  INFO - Testing query: query_nearest_with_geometry, starting at 20260205_15-43-52
2026-02-05 15:43:52 - testscript:84 - DEBUG - Reading CSV: /var/www/nginx/referencedata/aqua90m/fish_all_species_snapped_removed_empties.csv
2026-02-05 15:43:52 - testscript:93 - DEBUG - Connect to database...
2026-02-05 15:43:52 - database_connection:28 -  INFO - Database-Emergency-Off not configured (config file not found), using default (False).
2026-02-05 15:43:53 - temp_table_for_queries:105 - DEBUG - Preparing to insert data from a dataframe into PostGIS database...
2026-02-05 15:43:53 - temp_table_for_queries:138 - DEBUG - Created list of 3621 insert rows...
2026-02-05 15:43:53 - temp_table_for_queries:139 - DEBUG - First insert row: ('3POTAM', 22.19779814, 40.53310984, ST_SetSRID(ST_MakePoint(22.19779814, 40.53310984), 4326))
2026-02-05 15:43:53 - temp_table_for_queries:149 - DEBUG - Creating and populating temp table "pygeo_50b5f70a6db84bce9c112f9b1b60c3d3"...
2026-02-05 15:43:53 - temp_table_for_queries:179 - DEBUG - Creating temporary table "pygeo_50b5f70a6db84bce9c112f9b1b60c3d3"...
2026-02-05 15:43:53 - temp_table_for_queries:200 - DEBUG - Creating temporary table "pygeo_50b5f70a6db84bce9c112f9b1b60c3d3"... done.
2026-02-05 15:43:53 - temp_table_for_queries:205 - DEBUG - Inserting into temporary table "pygeo_50b5f70a6db84bce9c112f9b1b60c3d3"...
2026-02-05 15:43:53 - temp_table_for_queries:216 - DEBUG - Inserting into temporary table "pygeo_50b5f70a6db84bce9c112f9b1b60c3d3"... done.
2026-02-05 15:43:53 - temp_table_for_queries:220 - DEBUG - Creating index for temporary table "pygeo_50b5f70a6db84bce9c112f9b1b60c3d3"...
2026-02-05 15:43:53 - temp_table_for_queries:230 - DEBUG - Creating index for temporary table "pygeo_50b5f70a6db84bce9c112f9b1b60c3d3"... done.
2026-02-05 15:43:53 - temp_table_for_queries:236 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_50b5f70a6db84bce9c112f9b1b60c3d3"...
2026-02-05 15:43:54 - temp_table_for_queries:254 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_50b5f70a6db84bce9c112f9b1b60c3d3"... done
2026-02-05 15:43:54 - temp_table_for_queries:265 - DEBUG - Set of distinct reg_ids present in the temp table: {66, 59}
2026-02-05 15:43:54 - temp_table_for_queries:271 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_50b5f70a6db84bce9c112f9b1b60c3d3"...
2026-02-05 15:43:55 - temp_table_for_queries:289 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_50b5f70a6db84bce9c112f9b1b60c3d3"... done.
2026-02-05 15:43:55 - temp_table_for_queries:166 - DEBUG - Populating temp table "pygeo_50b5f70a6db84bce9c112f9b1b60c3d3" (incl. subc_id, basin_id, reg_id)... done.
2026-02-05 15:43:55 - testscript:126 -  INFO - Starting query: Nearest Neigbours
2026-02-05 15:46:58 - testscript:130 -  INFO - Finished query: Nearest Neigbours
2026-02-05 15:46:58 - testscript:131 - DEBUG - **** TIME ************: 183.77636861801147
2026-02-05 15:46:58 - testscript:210 -  INFO - Starting query: Snapping without distance
2026-02-05 15:46:58 - testscript:214 -  INFO - Finished query: Snapping without distance
2026-02-05 15:46:58 - testscript:215 - DEBUG - **** TIME ************: 0.026462554931640625
2026-02-05 15:46:58 - testscript:222 - DEBUG - Making dataframe from database result...
2026-02-05 15:46:58 - snapping_strahler:691 - DEBUG - Generating dataframe to return...
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:58 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:46:58 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:46:59 - testscript:224 - DEBUG - Storing dataframe to csv: test_query_nearest_with_geometry_20260205_15-43-52.csv...
2026-02-05 15:46:59 - temp_table_for_queries:31 - DEBUG - Dropping temporary table "pygeo_50b5f70a6db84bce9c112f9b1b60c3d3"...
2026-02-05 15:46:59 - temp_table_for_queries:34 - DEBUG - Dropping temporary table "pygeo_50b5f70a6db84bce9c112f9b1b60c3d3"... done.
2026-02-05 15:46:59 - testscript:230 -  INFO - Tested query: query_nearest_with_geometry, started  at 20260205_15-43-52
2026-02-05 15:46:59 - testscript:232 -  INFO - Tested query: query_nearest_with_geometry, finished at 20260205_15-46-59
2026-02-05 15:46:59 - testscript:233 -  INFO - Done.
```

TODO: Apparently there are some NAs! But we can already see the speed of the snapping queries.

### On-the-fly conversion to geography (small table)

```
WIP

(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ python testscript_snapping_convert_to_geography_onthefly_subset66.py 
2026-02-04 16:36:47 - testscript:75 -  INFO - Testing query: query_nearest_with_geography_66, starting at 20260204_16-36-47
2026-02-04 16:36:47 - testscript:85 - DEBUG - Reading CSV: /var/www/nginx/referencedata/aqua90m/fish_all_species_snapped_removed_empties.csv
2026-02-04 16:36:47 - testscript:94 - DEBUG - Connect to database...
2026-02-04 16:36:47 - database_connection:28 -  INFO - Database-Emergency-Off not configured (config file not found), using default (False).
2026-02-04 16:36:47 - temp_table_for_queries:105 - DEBUG - Preparing to insert data from a dataframe into PostGIS database...
2026-02-04 16:36:47 - temp_table_for_queries:138 - DEBUG - Created list of 3621 insert rows...
2026-02-04 16:36:47 - temp_table_for_queries:139 - DEBUG - First insert row: ('3POTAM', 22.19779814, 40.53310984, ST_SetSRID(ST_MakePoint(22.19779814, 40.53310984), 4326))
2026-02-04 16:36:47 - temp_table_for_queries:149 - DEBUG - Creating and populating temp table "pygeo_b8c72af275a941dbaace4cba49d54baf"...
2026-02-04 16:36:47 - temp_table_for_queries:179 - DEBUG - Creating temporary table "pygeo_b8c72af275a941dbaace4cba49d54baf"...
2026-02-04 16:36:47 - temp_table_for_queries:200 - DEBUG - Creating temporary table "pygeo_b8c72af275a941dbaace4cba49d54baf"... done.
2026-02-04 16:36:47 - temp_table_for_queries:205 - DEBUG - Inserting into temporary table "pygeo_b8c72af275a941dbaace4cba49d54baf"...
2026-02-04 16:36:47 - temp_table_for_queries:216 - DEBUG - Inserting into temporary table "pygeo_b8c72af275a941dbaace4cba49d54baf"... done.
2026-02-04 16:36:47 - temp_table_for_queries:220 - DEBUG - Creating index for temporary table "pygeo_b8c72af275a941dbaace4cba49d54baf"...
2026-02-04 16:36:47 - temp_table_for_queries:230 - DEBUG - Creating index for temporary table "pygeo_b8c72af275a941dbaace4cba49d54baf"... done.
2026-02-04 16:36:47 - temp_table_for_queries:236 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_b8c72af275a941dbaace4cba49d54baf"...
2026-02-04 16:36:48 - temp_table_for_queries:254 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_b8c72af275a941dbaace4cba49d54baf"... done
2026-02-04 16:36:48 - temp_table_for_queries:265 - DEBUG - Set of distinct reg_ids present in the temp table: {66, 59}
2026-02-04 16:36:48 - temp_table_for_queries:271 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_b8c72af275a941dbaace4cba49d54baf"...
2026-02-04 16:36:49 - temp_table_for_queries:289 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_b8c72af275a941dbaace4cba49d54baf"... done.
2026-02-04 16:36:49 - temp_table_for_queries:166 - DEBUG - Populating temp table "pygeo_b8c72af275a941dbaace4cba49d54baf" (incl. subc_id, basin_id, reg_id)... done.
2026-02-04 16:36:49 - testscript:127 -  INFO - Starting query: Nearest Neigbours

... interrupted at 18:01...
```

### Pre-converted geography column (test table)

```
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ python testscript_snapping_preconverted_geography_subset66_vanessa.py
2026-02-05 15:51:01 - testscript:75 -  INFO - Testing query: query_nearest_with_geography_VB, starting at 20260205_15-51-01
2026-02-05 15:51:01 - testscript:85 - DEBUG - Reading CSV: /var/www/nginx/referencedata/aqua90m/fish_all_species_snapped_removed_empties.csv
2026-02-05 15:51:01 - testscript:94 - DEBUG - Connect to database...
2026-02-05 15:51:01 - database_connection:28 -  INFO - Database-Emergency-Off not configured (config file not found), using default (False).
2026-02-05 15:51:01 - temp_table_for_queries:105 - DEBUG - Preparing to insert data from a dataframe into PostGIS database...
2026-02-05 15:51:01 - temp_table_for_queries:138 - DEBUG - Created list of 3621 insert rows...
2026-02-05 15:51:01 - temp_table_for_queries:139 - DEBUG - First insert row: ('3POTAM', 22.19779814, 40.53310984, ST_SetSRID(ST_MakePoint(22.19779814, 40.53310984), 4326))
2026-02-05 15:51:01 - temp_table_for_queries:149 - DEBUG - Creating and populating temp table "pygeo_901ac2895f294d6a93361423660679c7"...
2026-02-05 15:51:01 - temp_table_for_queries:179 - DEBUG - Creating temporary table "pygeo_901ac2895f294d6a93361423660679c7"...
2026-02-05 15:51:01 - temp_table_for_queries:200 - DEBUG - Creating temporary table "pygeo_901ac2895f294d6a93361423660679c7"... done.
2026-02-05 15:51:01 - temp_table_for_queries:205 - DEBUG - Inserting into temporary table "pygeo_901ac2895f294d6a93361423660679c7"...
2026-02-05 15:51:01 - temp_table_for_queries:216 - DEBUG - Inserting into temporary table "pygeo_901ac2895f294d6a93361423660679c7"... done.
2026-02-05 15:51:01 - temp_table_for_queries:220 - DEBUG - Creating index for temporary table "pygeo_901ac2895f294d6a93361423660679c7"...
2026-02-05 15:51:01 - temp_table_for_queries:230 - DEBUG - Creating index for temporary table "pygeo_901ac2895f294d6a93361423660679c7"... done.
2026-02-05 15:51:01 - temp_table_for_queries:236 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_901ac2895f294d6a93361423660679c7"...
2026-02-05 15:51:02 - temp_table_for_queries:254 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_901ac2895f294d6a93361423660679c7"... done
2026-02-05 15:51:02 - temp_table_for_queries:265 - DEBUG - Set of distinct reg_ids present in the temp table: {66, 59}
2026-02-05 15:51:02 - temp_table_for_queries:271 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_901ac2895f294d6a93361423660679c7"...
2026-02-05 15:51:03 - temp_table_for_queries:289 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_901ac2895f294d6a93361423660679c7"... done.
2026-02-05 15:51:03 - temp_table_for_queries:166 - DEBUG - Populating temp table "pygeo_901ac2895f294d6a93361423660679c7" (incl. subc_id, basin_id, reg_id)... done.
2026-02-05 15:51:03 - testscript:127 -  INFO - Starting query: Nearest Neigbours
2026-02-05 15:51:08 - testscript:131 -  INFO - Finished query: Nearest Neigbours
2026-02-05 15:51:08 - testscript:132 - DEBUG - **** TIME ************: 5.092331171035767
2026-02-05 15:51:08 - testscript:211 -  INFO - Starting query: Snapping without distance
2026-02-05 15:51:08 - testscript:215 -  INFO - Finished query: Snapping without distance
2026-02-05 15:51:08 - testscript:216 - DEBUG - **** TIME ************: 0.017423152923583984
2026-02-05 15:51:08 - testscript:223 - DEBUG - Making dataframe from database result...
2026-02-05 15:51:08 - snapping_strahler:691 - DEBUG - Generating dataframe to return...
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - snapping_strahler:727 - WARNING - Could not parse lon and lat: Both are None...
2026-02-05 15:51:08 - snapping_strahler:745 - DEBUG - This point has no ids assigned, so it may be off the coast: site_id=P266, lon=None, lat=None.
2026-02-05 15:51:08 - testscript:225 - DEBUG - Storing dataframe to csv: test_query_nearest_with_geography_VB_20260205_15-51-01.csv...
2026-02-05 15:51:08 - temp_table_for_queries:31 - DEBUG - Dropping temporary table "pygeo_901ac2895f294d6a93361423660679c7"...
2026-02-05 15:51:08 - temp_table_for_queries:34 - DEBUG - Dropping temporary table "pygeo_901ac2895f294d6a93361423660679c7"... done.
2026-02-05 15:51:08 - testscript:231 -  INFO - Tested query: query_nearest_with_geography_VB, started  at 20260205_15-51-01
2026-02-05 15:51:08 - testscript:233 -  INFO - Tested query: query_nearest_with_geography_VB, finished at 20260205_15-51-08
2026-02-05 15:51:08 - testscript:234 -  INFO - Done.
```

### Pre-converted geography column (entire table)

Does not exist yet

### On-the-fly conversion to geography (super slow)

We're not even trying this...


## Continued 20 February 2026

### Fifth test

This is the same as the Vanessa-Test (with predefined geography columns, on the test table), BUT I also changed the temp table columns
to geography (so I had to redefine/override the temp table creation functions).

This is to see whether distance computation is faster or not based on a geography temp table.

Because in the previous test, I used Vanessa's geography-columns to find the nearest neighbours, but then the
temp table was all geometry-columns, so computing distance there does not yield any new insights...


Insights:

Interestingly, here, getting the `reg_id` takes a long time:

```
2026-02-20 20:37:47 - testscript:359 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_2fee74295a9941139eed4e0caac9c5ee"...
2026-02-20 20:49:06 - testscript:377 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_2fee74295a9941139eed4e0caac9c5ee"... done
```

Sadly, I run into this error: `function st_linelocatepoint(geography, geography) does not exist`.
It is weird, because the documentation for `ST_LineLocatePoint()` explicitly states that geography and
geography are valid inputs: https://postgis.net/docs/ST_LineLocatePoint.html

```
Synopsis
float8 ST_LineLocatePoint(geometry a_linestring, geometry a_point);
float8 ST_LineLocatePoint(geography a_linestring, geography a_point, boolean use_spheroid = true);
# source. https://postgis.net/docs/ST_LineLocatePoint.html
```

Maybe I have to set the `use_spheroid` boolean? But it has a default. Plus, it is not explained, so
I am not sure what I'd have to set...
Anyway, next attempt with setting it to `true` which is the default anyway...

Log output:

```
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ python testscript_snapping_preconverted_geography_subset66_vanessa_geogtemp.py
2026-02-20 20:37:47 - testscript:82 -  INFO - Testing query: query_nearest_with_geography_VB, starting at 20260220_20-37-47
2026-02-20 20:37:47 - testscript:92 - DEBUG - Reading CSV: /var/www/nginx/referencedata/aqua90m/fish_all_species_snapped_removed_empties.csv
2026-02-20 20:37:47 - testscript:101 - DEBUG - Connect to database...
2026-02-20 20:37:47 - database_connection:28 -  INFO - Database-Emergency-Off not configured (config file not found), using default (False).
2026-02-20 20:37:47 - temp_table_for_queries:105 - DEBUG - Preparing to insert data from a dataframe into PostGIS database...
2026-02-20 20:37:47 - temp_table_for_queries:138 - DEBUG - Created list of 3621 insert rows...
2026-02-20 20:37:47 - temp_table_for_queries:139 - DEBUG - First insert row: ('3POTAM', 22.19779814, 40.53310984, ST_SetSRID(ST_MakePoint(22.19779814, 40.53310984), 4326))
2026-02-20 20:37:47 - testscript:261 - DEBUG - Creating and populating temp table "pygeo_2fee74295a9941139eed4e0caac9c5ee"...
2026-02-20 20:37:47 - testscript:294 - DEBUG - Creating temporary table "pygeo_2fee74295a9941139eed4e0caac9c5ee"...
2026-02-20 20:37:47 - testscript:315 - DEBUG - Creating temporary table "pygeo_2fee74295a9941139eed4e0caac9c5ee"... done.
2026-02-20 20:37:47 - testscript:322 - DEBUG - Inserting into temporary table "pygeo_2fee74295a9941139eed4e0caac9c5ee"...
2026-02-20 20:37:47 - testscript:333 - DEBUG - Inserting into temporary table "pygeo_2fee74295a9941139eed4e0caac9c5ee"... done.
2026-02-20 20:37:47 - testscript:339 - DEBUG - Creating index for temporary table "pygeo_2fee74295a9941139eed4e0caac9c5ee"...
2026-02-20 20:37:47 - testscript:349 - DEBUG - Creating index for temporary table "pygeo_2fee74295a9941139eed4e0caac9c5ee"... done.
2026-02-20 20:37:47 - testscript:359 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_2fee74295a9941139eed4e0caac9c5ee"...
2026-02-20 20:49:06 - testscript:377 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_2fee74295a9941139eed4e0caac9c5ee"... done
2026-02-20 20:49:06 - testscript:388 - DEBUG - Set of distinct reg_ids present in the temp table: {66, 59}
2026-02-20 20:49:06 - testscript:394 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_2fee74295a9941139eed4e0caac9c5ee"...
2026-02-20 20:52:22 - testscript:412 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_2fee74295a9941139eed4e0caac9c5ee"... done.
2026-02-20 20:52:22 - testscript:278 - DEBUG - Populating temp table "pygeo_2fee74295a9941139eed4e0caac9c5ee" (incl. subc_id, basin_id, reg_id)... done.
2026-02-20 20:52:22 - testscript:136 -  INFO - Starting query: Nearest Neigbours
2026-02-20 20:52:33 - testscript:140 -  INFO - Finished query: Nearest Neigbours
2026-02-20 20:52:33 - testscript:141 - DEBUG - **** TIME ************: 10.751922130584717
2026-02-20 20:52:33 - testscript:161 -  INFO - Starting query: Snapping
Traceback (most recent call last):
  File "/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh/testscript_snapping_preconverted_geography_subset66_vanessa_geogtemp.py", line 419, in <module>
    main()
  File "/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh/testscript_snapping_preconverted_geography_subset66_vanessa_geogtemp.py", line 163, in main
    cursor.execute(query)
psycopg2.errors.UndefinedFunction: function st_linelocatepoint(geography, geography) does not exist
LINE 5:                 ST_LineLocatePoint(temp.geog_closest, temp.g...
                        ^
HINT:  No function matches the given name and argument types. You might need to add explicit type casts.

(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ 
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ 
```

Now with adaptation:

```
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ 
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ 
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ git diff tests_snapping_methods_20260204/testscript_snapping_preconverted_geography_subset66_vanessa_geogtemp.py
diff --git a/geofresh/tests_snapping_methods_20260204/testscript_snapping_preconverted_geography_subset66_vanessa_geogtemp.py b/geofresh/tests_snapping_methods_20260204/testscript_snapping_preconverted_geography_subset66_vanessa_geogtemp.py
index 1a0ed61..b53fa30 100644
--- a/geofresh/tests_snapping_methods_20260204/testscript_snapping_preconverted_geography_subset66_vanessa_geogtemp.py
+++ b/geofresh/tests_snapping_methods_20260204/testscript_snapping_preconverted_geography_subset66_vanessa_geogtemp.py
@@ -213,7 +218,7 @@ FP2,20.7915502371247,40.1392343125345,560164915,1292502,66"""
             ST_AsText(
                 ST_LineInterpolatePoint(
                     temp.geog_closest,
-                    ST_LineLocatePoint(temp.geog_closest, temp.geog_user)
+                    ST_LineLocatePoint(temp.geog_closest, temp.geog_user, true)
                 )
             ),
             temp.strahler_closest,
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ 
```

But still the same error:

```
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ 
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ cp tests_snapping_methods_20260204/testscript_snapping_preconverted_geography_subset66_vanessa_geogtemp.py ./
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ python testscript_snapping_preconverted_geography_subset66_vanessa_geogtemp.py
2026-02-20 21:02:52 - testscript:83 -  INFO - Testing query: query_nearest_with_geography_VB, starting at 20260220_21-02-52
2026-02-20 21:02:52 - testscript:93 - DEBUG - Reading CSV: /var/www/nginx/referencedata/aqua90m/fish_all_species_snapped_removed_empties.csv
2026-02-20 21:02:52 - testscript:102 - DEBUG - Connect to database...
2026-02-20 21:02:52 - database_connection:28 -  INFO - Database-Emergency-Off not configured (config file not found), using default (False).
2026-02-20 21:02:52 - temp_table_for_queries:105 - DEBUG - Preparing to insert data from a dataframe into PostGIS database...
2026-02-20 21:02:52 - temp_table_for_queries:138 - DEBUG - Created list of 3621 insert rows...
2026-02-20 21:02:52 - temp_table_for_queries:139 - DEBUG - First insert row: ('3POTAM', 22.19779814, 40.53310984, ST_SetSRID(ST_MakePoint(22.19779814, 40.53310984), 4326))
2026-02-20 21:02:52 - testscript:262 - DEBUG - Creating and populating temp table "pygeo_a353fad431704252bb657db5ad5814db"...
2026-02-20 21:02:52 - testscript:295 - DEBUG - Creating temporary table "pygeo_a353fad431704252bb657db5ad5814db"...
2026-02-20 21:02:52 - testscript:316 - DEBUG - Creating temporary table "pygeo_a353fad431704252bb657db5ad5814db"... done.
2026-02-20 21:02:52 - testscript:323 - DEBUG - Inserting into temporary table "pygeo_a353fad431704252bb657db5ad5814db"...
2026-02-20 21:02:52 - testscript:334 - DEBUG - Inserting into temporary table "pygeo_a353fad431704252bb657db5ad5814db"... done.
2026-02-20 21:02:52 - testscript:340 - DEBUG - Creating index for temporary table "pygeo_a353fad431704252bb657db5ad5814db"...
2026-02-20 21:02:52 - testscript:350 - DEBUG - Creating index for temporary table "pygeo_a353fad431704252bb657db5ad5814db"... done.
2026-02-20 21:02:52 - testscript:360 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_a353fad431704252bb657db5ad5814db"...
2026-02-20 21:14:14 - testscript:378 - DEBUG - Update reg_id (st_intersects) in temporary table "pygeo_a353fad431704252bb657db5ad5814db"... done
2026-02-20 21:14:14 - testscript:389 - DEBUG - Set of distinct reg_ids present in the temp table: {66, 59}
2026-02-20 21:14:14 - testscript:395 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_a353fad431704252bb657db5ad5814db"...
2026-02-20 21:17:11 - testscript:413 - DEBUG - Update subc_id, basin_id (st_intersects) in temporary table "pygeo_a353fad431704252bb657db5ad5814db"... done.
2026-02-20 21:17:11 - testscript:279 - DEBUG - Populating temp table "pygeo_a353fad431704252bb657db5ad5814db" (incl. subc_id, basin_id, reg_id)... done.
2026-02-20 21:17:11 - testscript:137 -  INFO - Starting query: Nearest Neigbours
2026-02-20 21:17:21 - testscript:141 -  INFO - Finished query: Nearest Neigbours
2026-02-20 21:17:21 - testscript:142 - DEBUG - **** TIME ************: 10.836235523223877
2026-02-20 21:17:21 - testscript:162 -  INFO - Starting query: Snapping
Traceback (most recent call last):
  File "/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh/testscript_snapping_preconverted_geography_subset66_vanessa_geogtemp.py", line 420, in <module>
    main()
  File "/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh/testscript_snapping_preconverted_geography_subset66_vanessa_geogtemp.py", line 164, in main
    cursor.execute(query)
psycopg2.errors.UndefinedFunction: function st_linelocatepoint(geography, geography) does not exist
LINE 5:                 ST_LineLocatePoint(temp.geog_closest, temp.g...
                        ^
HINT:  No function matches the given name and argument types. You might need to add explicit type casts.

(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ 
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ 
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ 
(venv) mbuurman@aqua:/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/aqua90m/geofresh$ 

```

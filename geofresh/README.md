
# Overview GeoFRESH functionality

## Contents

* [Prep (from lonlat)](#prep-from-lonlat-first-get-reg_id-then-get-subc_id-basin_id)
* [Prep (from subc_id)](#prep-from-subc_id-get-reg_id-basin_id-in-one-step)
* [Simple: Polygon](#simple-polygon)
* [Simple: Linestring](#simple-linestring)
* [Upstream](#upstream)
* [Dissolved](#dissolved)
* [Bbox](#bbox)
* [Snapping](#snapping)
* [Routing (Dijkstra)](#routing-dijkstra)


## Preparatory steps

Basically all GeoFRESH functionality needs a subcatchment id (`subc_id`) as input - usually along with a basin id (`basin_id`) and a regional unit id (`reg_id`), to speed up querying from the database.

Most of the times, the user will have specified some location (a WGS84 coordinate pair of latitude and longitude, let's call it `lonlat`). In some cases, the user - familiar with the Hydrography90m dataset - will have specified a `subc_id` directly.

So there are two ways to start any analysis:

* If user inputs `lonlat`: First get `reg_id` from one table, then get `basin_id` and `subc_id` from another.
* If user inputs `sub_id`: Get `basin_id` and `reg_id` in one step.


### Prep (from lonlat): First get reg_id, then get subc_id, basin_id

|         |                |
|---------|----------------|
| input   | `lonlat`       |
| output  | `reg_id`       |
| table   | regional_units |
| PostGIS | st_intersects  |


|         |                       |
|---------|-----------------------|
| input   | `lonlat`, `reg_id`    |
| output  | `subc_id`, `basin_id` |
| table   | sub_catchments        |
| PostGIS | st_intersects         |


### Prep (from subc_id): Get reg_id, basin_id in one step:

|         |                      |
|---------|----------------------|
| input   | `subc_id`            |
| output  | `reg_id`, `basin_id` |
| table   | sub_catchments       |
| PostGIS | (none)               |



## Simple queries

These are simple queries that return the geometry that belongs with a subc_id. This can be either a polygon (representing the subcatchment area) or a linestring (representing the stream segment).

When we query for the linestring (table hydro.stream_segments), we can also get all the other attributes stored in that table (strahler, target, length, cum_length, flow_accum).

When we query for the polygon (table sub_catchments), we can also get all the other attributes stored in that table (subc_id , basin_id, reg_id - these we already have! TODO: Check, no other attributes?).


### Simple: Polygon

|         |                |
|---------|----------------|
| input   | 1 `subc_id`    |
| output  | 1 polygon      |
| output attributes | any of table sub_catchments |
| table   | sub_catchments |
| PostGIS | (none)         |

Note: The output may contain any attribute of the table sub_catchments (subc_id, basin_id, reg_id).


### Simple: Linestring

|         |                       |
|---------|-----------------------|
| input   | 1 `subc_id`           |
| output  | 1 linestring          |
| output attributes | any of table stream_segments |
| table   | hydro.stream_segments |
| PostGIS | (none)                |

Note: The output may contain any attribute of the table hydro.stream_segments (strahler, target, length, cum_length, flow_accum).   


## Computations for the upstream catchments

### Upstream

|         |                         |
|---------|-------------------------|
| input   | 1 `subc_id`             |
| output  | n `subc_ids`            |
| table   | hydro.stream_segments   |
| output attributes | None?         |
| PostGIS | pgr_connectedComponents |

Note on outputs: (TODO) pgr_connectedComponents does not seem to produce any output but the ids, but we can verify.



### Dissolved

This functions works on any set of subc_ids, but it probably only makes sense of those form one upstream catchment...

|         |                 |
|---------|-----------------|
| input   | n `subc_ids`    |
| output  | 1 polygon       |
| output attributes | None? |
| table   | sub_catchments  |
| PostGIS | ST_MemUnion     |

Note on outputs: (TODO) ST_MemUnion does not seem to produce any output but the geometry, but we can verify.


### Bbox

This functions works on any set of subc_ids, but it probably only makes sense of those form one upstream catchment...

|         |                  |
|---------|------------------|
| input   | n `subc_ids`     |
| output  | 1 polygon (bbox) |
| output attributes | None?  |
| table   | sub_catchments   |
| PostGIS | ST_Extent        |

Note on outputs: (TODO) ST_Extent does not seem to produce any output but the geometry, but we can verify.



## Snapping

|         |                         |
|---------|-------------------------|
| input   | 1 `lonlat`, 1 `subc_id` |
| output  | 1 point, 1 linestring   |
| output attributes | any of table stream_segments |
| table   | hydro.stream_segments   |
| PostGIS | ST_LineInterpolatePoint, ST_LineLocatePoint |

Note: The output may contain any attribute of the table hydro.stream_segments (strahler, target, length, cum_length, flow_accum).

Note: This function needs a `lonlat` AND a `subc_id` as input, as it snaps the `lonlat` towards the stream segment specified by `subc_id`. So in theory, we could pass the `subc_id` of a different location than `lonlat`, to snap it e.g. to a stream segment with a higher strahler order.

Note to developers: For running this on many inputs, we have to create a temporary table, or run it in a loop (because ST_MakePoint(lon, lat) does not take several points as input).


## Routing (Dijkstra)

|         |                                                        |
|---------|--------------------------------------------------------|
| input   | 2 `subcids` (start and end point)                      |
| output  | n `subcids` (the path between the start and end point) |
| output attributes | `subcid`, length, aggregated length, ...     |
| table   | hydro.stream_segments |
| PostGIS | pgr_dijkstra          |

Routing input: Edges, which have start and end id (subc_ids), and a cost (which is length). So as a result we can also get subc_id, and length. For more details, read the [documentation of pgr_dijkstra](https://docs.pgrouting.org/main/en/pgr_dijkstra.html).



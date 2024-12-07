# aqua90m

Remote interaction with freshwater and related data, mainly via OGC processes
deployed on pygeoapi instances.


## List of processes

local subcatchment

* get_snapped_points
* get_snapped_points_plus
* get_local_subcids
* get_local_subcids_plural
* get_local_streamsegments
* get_local_streamsegments_subcatchments

upstream

* get_upstream_subcids
* get_upstream_streamsegments
* get_upstream_bbox
* get_upstream_subcatchments
* get_upstream_dissolved
* get_upstream_dissolved_aip (special version for usage by the AIP search interface, kept constant)

downstream

* get_shortest_path_two_points
* get_shortest_path_to_outlet

data access

* extract_point_stats
* subset_by_polygon (currently fails on aqua due to dependency issues with gdal)
* subset_by_bbox (currently fails on aqua due to dependency issues with gdal)

utils

* get_ddas_galaxy_link_textfile


## Pygeoapi deployment

To deploy these processes, you first need a running pygeoapi instance (please
see https://pygeoapi.io/ and follow their recommendations and best practices,
e.g. about web servers, reverse-proxy usage and TLS certificates / HTTPS). We
run pygeoapi via starlette behing an nginx webserver, via HTTPS.

To deploy the processes listed above on this pygeoapi instance, please modify
the following existing files:

File `pygeoapi/pygeoapi/plugin.py`:

```
        # local subcatchment
        'SnappedPointsGetter': 'pygeoapi.process.aqua90m.pygeoapi_processes.rivernetwork.get_snapped_points.SnappedPointsGetter',
        'SnappedPointsGetterPlus': 'pygeoapi.process.aqua90m.pygeoapi_processes.rivernetwork.get_snapped_points_plus.SnappedPointsGetterPlus',
        'LocalSubcidGetter': 'pygeoapi.process.aqua90m.pygeoapi_processes.rivernetwork.get_local_subcids.LocalSubcidGetter',
        'LocalSubcidPluralGetter': 'pygeoapi.process.aqua90m.pygeoapi_processes.rivernetwork.get_local_subcids_plural.LocalSubcidPluralGetter',
        'LocalStreamSegmentsGetter': 'pygeoapi.process.aqua90m.pygeoapi_processes.rivernetwork.get_local_streamsegments.LocalStreamSegmentsGetter',
        'LocalStreamSegmentSubcatchmentGetter': 'pygeoapi.process.aqua90m.pygeoapi_processes.rivernetwork.get_local_streamsegments_subcatchments.LocalStreamSegmentSubcatchmentGetter',
        # upstream
        'UpstreamSubcidGetter': 'pygeoapi.process.aqua90m.pygeoapi_processes.rivernetwork.get_upstream_subcids.UpstreamSubcidGetter',
        'UpstreamStreamSegmentsGetter': 'pygeoapi.process.aqua90m.pygeoapi_processes.rivernetwork.get_upstream_streamsegments.UpstreamStreamSegmentsGetter',
        'UpstreamBboxGetter': 'pygeoapi.process.aqua90m.pygeoapi_processes.rivernetwork.get_upstream_bbox.UpstreamBboxGetter',
        'UpstreamSubcatchmentGetter': 'pygeoapi.process.aqua90m.pygeoapi_processes.rivernetwork.get_upstream_subcatchments.UpstreamSubcatchmentGetter',
        'UpstreamDissolvedGetterCont': 'pygeoapi.process.aqua90m.pygeoapi_processes.rivernetwork.get_upstream_dissolved.UpstreamDissolvedGetter',
        'UpstreamDissolvedGetter': 'pygeoapi.process.aqua90m.pygeoapi_processes.rivernetwork.get_upstream_dissolved_aip.UpstreamDissolvedGetter',
        # downstream
        'ShortestPathTwoPointsGetter': 'pygeoapi.process.aqua90m.pygeoapi_processes.rivernetwork.get_shortest_path_two_points.ShortestPathTwoPointsGetter',
        'ShortestPathToOutletGetter': 'pygeoapi.process.aqua90m.pygeoapi_processes.rivernetwork.get_shortest_path_to_outlet.ShortestPathToOutletGetter',
        # data access
        'ExtractPointStatsProcessor': 'pygeoapi.process.aqua90m.pygeoapi_processes.data_access.extract_point_stats.ExtractPointStatsProcessor',
        #'SubsetterBbox': 'pygeoapi.process.aqua90m.pygeoapi_processes.data_access.subset_by_bbox.SubsetterBbox',
        #'SubsetterPolygon': 'pygeoapi.process.aqua90m.pygeoapi_processes.data_access.subset_by_polygon.SubsetterPolygon',
        # utils
        'HelferleinProcessor': 'pygeoapi.process.aqua90m.pygeoapi_processes.data_access.get_ddas_galaxy_link_textfile.HelferleinProcessor',
```

File `pygeoapi/pygeoapi-config.yml`:


```
   # local subcatchment

    get-snapped-points:
        type: process
        processor:
            name: SnappedPointsGetter

    get-snapped-point-plus:
        type: process
        processor:
            name: SnappedPointsGetterPlus

    get-local-subcids:
        type: process
        processor:
            name: LocalSubcidGetter

    get-local-subcids-plural:
        type: process
        processor:
            name: LocalSubcidPluralGetter

    get-local-streamsegments:
        type: process
        processor:
            name: LocalStreamSegmentsGetter

    get-local-streamsegments-subcatchments:
        type: process
        processor:
            name: LocalStreamSegmentSubcatchmentGetter



    # upstream

    get-upstream-subcids:
        type: process
        processor:
            name: UpstreamSubcidGetter

    get-upstream-streamsegments:
        type: process
        processor:
            name: UpstreamStreamSegmentsGetter

    get-upstream-bbox:
        type: process
        processor:
            name: UpstreamBboxGetter

    get-upstream-subcatchments:
        type: process
        processor:
            name: UpstreamSubcatchmentGetter

    get-upstream-dissolved:
        type: process
        processor:
            name: UpstreamDissolvedGetter

    get-upstream-dissolved-cont:
        type: process
        processor:
            name: UpstreamDissolvedGetterCont



    # downstream

    get-shortest-path-two-points:
        type: process
        processor:
            name: ShortestPathTwoPointsGetter

    get-shortest-path-to-outlet:
        type: process
        processor:
            name: ShortestPathToOutletGetter



    # data access

    extract-point-stats:
        type: process
        processor:
            name: ExtractPointStatsProcessor

    #FAILS (GDAL) get-subset-by-bbox:
        #type: process
        #processor:
              #name: SubsetterBbox

    # FAILS (GDAL) get-subset-by-polygon:
        #type: process
        #processor:
              #name: SubsetterPolygon



    # utils
    get-ddas-galaxy-link-textfile:
        type: process
        processor:
            name: HelferleinProcessor
```

Dependencies, to be added to `dependencies.txt`

```
TODO
# Also, how to install them?
```

In addition, you need to add a JSON config file for process-relevant config,
located at any readable place, e.g. `pygeoapi/config.json`. The contents vary
by processes (see process-specific details), but some entries are shared by all
or many processes, such as:

* `download_dir`: The path to a directory from where users are able to download
  files, e.g. the static directory of the webserver that is running pygeoapi.
  In our case, it is `/var/www/nginx/download/`. It needs to be writeable by the
  Linux user who runs pygeoapi (in our case, `pyguser`), and readable by the
  Linux user who runs the webserver (in our case, `www-data`).
* `download_url`: The URL under which the contents of the above `download_dir`
  can be accessed by outside users. Here, the process results will be made
  available to the end users. (You can decide to make the directory
  password-protected by the web server if the results should not be public).


For many of the processes, you also need credentials to access to IGB's
**GeoFRESH** database, which is restricted. For more information, please check
https://geofresh.org/ or contact IGB Berlin.

For some other processes, R and the R package `hydrographr` need to be installed
and runnable by the Linux user running pygeoapi.


## Process-specific details

### extract-point-stats

For this process, R and the R package `hydrographr` are needed.

For this process, the config file needs to contain these items:

* rasterlayer_lookup_table: Mapping between variable names and a local path or
  remote URL where the corresponding raster layer can be found, as GeoTIFF or
  VRT or any layer that `gdallocationinfo` can work with.
* hydrographr_bash_files: Path where the executable bash files of the
  `hydrographr` R package can be found.
* download_dir
* download_url


Example:

```
    "rasterlayer_lookup_table": {
        "basin": "https://2007367-nextcloud.a3s.fi/igb/vrt/basin.vrt",
        "sti": "/opt/aquainfra_inputs/Hydrography90m/sti_h18v02.tif"
    },
    "hydrographr_bash_files": "/opt/pyg_upstream_dev/pygeoapi/pygeoapi/process/hydrographr/inst/sh",
    "download_dir": "/var/www/nginx/download/",
    "download_url": "https://aqua.igb-berlin.de/download/",


```

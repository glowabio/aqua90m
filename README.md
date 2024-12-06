# aqua90m

Remote interaction with freshwater and related data, mainly via OGC processes
deployed on pygeoapi instances.


## List of processes

(TODO)


## Pygeoapi deployment

To deploy these processes, you first need a running pygeoapi instance (please
see https://pygeoapi.io/ and follow their recommendations and best practices,
e.g. about web servers, reverse-proxy usage and TLS certificates / HTTPS). We
run pygeoapi via starlette behing an nginx webserver, via HTTPS.

To deploy the processes listed above on this pygeoapi instance, please modify
the following existing files:

File `pygeoapi/pygeoapi/plugin.py`:

```
TODO
```

File `pygeoapi/pygeoapi-config.yml`:


```
TODO
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

(TODO)

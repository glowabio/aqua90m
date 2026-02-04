# Map client

Also known as the _"minesweeper"_...

## How to use

To function, the _minesweeper_ needs a chosen process and one (or two) point(s)
as WGS84 longitude and latitude value(s), or one (or two) subcatchment id(s).

The user can provide them via clicking on the map, via a form, or as URL
parameters:

* Click: Pick the desired process in the dropdown menu and click on the map at https://someserver.de/upstream/index.html
* Forms: Pick the desired process in the dropdown menu, fill the forms under the
  map with lon lat values (WGS84) or with subcatchment id(s)
* URL params: Add the desired process and the lon lat values to the URL, like
  this: `?lat=<latitude>&lon=<longitude>&processId=<process-id>`,
  for example: https://someserver.de/upstream/index.html?lat=40.3&lon=0.5&processId=get-upstream-bbox


## How to deploy

We are using nginx as a proxy.

The mapclient code sits in the git repository, located in the place where
pygeoapi expects the processes, for example:

```
/opt/wherever/pygeoapi/pygeoapi/process/aqua90m/mapclient/
```

In the nginx config:

```
        location /upstream/ {
                alias /var/www/nginx/upstream/mapclient/;
                index index.html;
        }

```

We use a softlink to make sure nginx can find it.

```
TODO: mapclient deployment: Add softlink command
TODO: couldn't we just provide the link to the git repo directly in the nginx config??
```

It should look like this:

```
user@server:/var/www/nginx/upstream$ ls -lpah
lrwxrwxrwx  1 root     root       66 Jan 20 19:32 mapclient -> /opt/wherever/pygeoapi/pygeoapi/process/aqua90m/mapclient/
```

TODO mapclient deployment: Permissions?? are these too open? what permissions are needed?

Please place the logos (`logo_aquainfra.svg` and `logo_igb.svg`) into the
`mapclient` directory (at 
`/opt/wherever/pygeoapi/pygeoapi/process/aqua90m/mapclient/`), otherwise they
will not be displayed.

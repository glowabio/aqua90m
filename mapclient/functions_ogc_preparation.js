
// Define making request to OGC service (function):
var ogcRequestTwoCoordinatePairs = function(map, lon1, lat1, lon2, lat2, processId, processDesc, logUserAction) {
    console.log('[DEBUG] Triggering suite of actions for two pairs of coordinates...');

    // Parse coordinates to Float
    var lat1 = parseFloat(lat1);
    var lon1 = parseFloat(lon1);
    var lat2 = parseFloat(lat2);
    var lon2 = parseFloat(lon2);

    // Add icon and popup to click location:
    clickMarker = putIconToClickLocation(lon1, lat1, map, logUserAction+' (part 1)');
    clickMarker = putIconToClickLocation(lon1, lat1, map, logUserAction+' (part 2)');
    let paramstring = lon1.toFixed(3)+", "+lat1.toFixed(3)+" (lon, lat) to "+lon2.toFixed(3)+", "+lat2.toFixed(3)+" (lon, lat)...";
    clickMarker.bindPopup("Waiting for "+processDesc+" for "+paramstring).openPopup();

    // Reset result field:
    document.getElementById("responseField").innerHTML = "Response returned by server for <span class=\"code\">"+lon1+", "+lat1+"</span> to <span class=\"code\">"+lon2+", "+lat2+"</span> (lon, lat, WGS84)...";
    document.getElementById("displayGeoJSON").innerHTML = "waiting..."

    // Define JSON payload and send:
    var payload_inputs_json = JSON.stringify({"inputs": {
      "point_start": {
        "type": "Point",
        "coordinates": [lon1, lat1],
      },
      "point_end": {
        "type": "Point",
        "coordinates": [lon2, lat2],
      }
    }})
    _ogcRequest(clickMarker, processId, processDesc, payload_inputs_json);
}

// Define making request to OGC service (function):
var ogcRequestOneCoordinatePair = function(map, lon1, lat1, processId, processDesc, logUserAction) {
    console.log('[DEBUG] Triggering suite of actions for one pair of coordinates...');

    // Parse coordinates to Float
    var lon1 = parseFloat(lon1);
    var lat1 = parseFloat(lat1);

    // Add icon and popup to click location:
    clickMarker = putIconToClickLocation(lon1, lat1, map, logUserAction);
    let paramstring = lon1.toFixed(3)+", "+lat1.toFixed(3)+" (lon, lat)...";
    clickMarker.bindPopup("Waiting for "+processDesc+" for "+paramstring).openPopup();

    // Reset result field:
    document.getElementById("responseField").innerHTML = "Response returned by server for lon, lat <span class=\"code\">"+lon1+", "+lat1+"</span> (lon, lat, WGS84)...";
    document.getElementById("displayGeoJSON").innerHTML = "waiting..."

    // If upstream, make pre-request:
    if (processId.startsWith("get-upstream")) {
      console.log('Requesting upstream... This may take a while, so we do a pre-request!');
      _preRequestUpstream(clickMarker, lon1, lat1);
    } else if (processId == 'get-shortest-path-to-outlet') {
      console.log('Requesting downstream... This may take a while, so we do a pre-request!');
      _preRequestDownstream(clickMarker, lon1, lat1);
    }

    // Define JSON payload and send:
    var payload_inputs_json = JSON.stringify({"inputs":{
      "point": {
        "type": "Point",
        "coordinates": [lon1, lat1]
      }
    }})
    _ogcRequest(clickMarker, processId, processDesc, payload_inputs_json);
}


// Define making request to OGC service (function):
var ogcRequestOneSubcid = function(map, subcid, processId, processDesc, logUserAction) {
    console.log('[DEBUG] Triggering suite of actions for one subc_id...');

    // Add icon and popup to click location:
    clickMarker = putIconToSubcidLocation(map, logUserAction);
    let paramstring = "subcatchment "+subcid+"...";
    clickMarker.bindPopup("Waiting for "+processDesc+" for "+paramstring).openPopup();

    // Reset result field:
    document.getElementById("responseField").innerHTML = "Response returned by server for subc_id <span class=\"code\">"+subcid+"</span>...";
    document.getElementById("displayGeoJSON").innerHTML = "waiting..."

    // Define JSON payload:
    var payload_inputs_json = JSON.stringify({"inputs":{"subc_id":subcid}})

    // If upstream, make pre-request:
    if (processId.startsWith("get-upstream")) {
      console.log('Requesting upstream... This may take a while, so we do a pre-request!');
      _preRequestUpstreamSubcid(clickMarker, subcid);
    } else if (processId == 'get-shortest-path-to-outlet') {
      console.log('Requesting downstream... This may take a while, so we do a pre-request!');
      _preRequestDownstreamSubcid(clickMarker, subcid);
    }

    // Send HTTP request to OGC service:
    _ogcRequest(clickMarker, processId, processDesc, payload_inputs_json);
}


// Define making request to OGC service (function):
var ogcRequestTwoSubcids = function(map, subcid1, subcid2, processId, processDesc, logUserAction) {
    console.log('[DEBUG] Triggering suite of actions for two subc_ids...');

    // Add icon and popup to some location:
    clickMarker = putIconToSubcidLocation(map, logUserAction);
    let paramstring = "subcatchments "+subcid1+" and "+subcid2+"...";
    clickMarker.bindPopup("Waiting for "+processDesc+" for "+paramstring).openPopup();

    // Reset result field:
    document.getElementById("responseField").innerHTML = "Response returned by server for subc_id <span class=\"code\">"+subcid1+"</span> to <span class=\"code\">"+subcid2+"...";
    document.getElementById("displayGeoJSON").innerHTML = "waiting..."

    // Define JSON payload:
    var payload_inputs_json = JSON.stringify({"inputs":{
      "subc_id_start":subcid1,
      "subc_id_end": subcid2
    }})

    // Send HTTP request to OGC service:
    _ogcRequest(clickMarker, processId, processDesc, payload_inputs_json);
}


// Define making request to OGC service (function):
var ogcRequestTwoMixed = function(map, lon, lat, subcid, processId, processDesc, logUserAction) {
    console.log('[DEBUG] Triggering suite of actions for one pair of coordinates and one subc_id (mixed)...');

    // Add icon and popup to some location:
    clickMarker = putIconToClickLocation(lon, lat, map, logUserAction);
    let paramstring = lon.toFixed(3)+", "+lat.toFixed(3)+" (lon, lat) to subcatchment "+subcid+"...";
    clickMarker.bindPopup("Waiting for "+processDesc+" for "+paramstring).openPopup();

    // Reset result field:
    document.getElementById("responseField").innerHTML = "Response returned by server for <span class=\"code\">"+lon+", "+lat+"</span> to <span class=\"code\">"+subcid+"...";
    document.getElementById("displayGeoJSON").innerHTML = "waiting..."

    // Define JSON payload:
    // Note: The coordinate pair is always treated as the "start", and subc_id always
    // as the "end"; the exact same request is sent in both "mixed" cases. Should the
    // order matter at some point, add another "ogcRequestTwoMixed()" function.
    var payload_inputs_json = JSON.stringify({
      "inputs": {
        "lon_start": lon,
        "lat_start": lat,
        "subc_id_end":subcid
      }
    })

    // Send HTTP request to OGC service:
    _ogcRequest(clickMarker, processId, processDesc, payload_inputs_json);
}

// Pre-Requests:
// If upstream or downstream things are requested, we first make
// a pre-request that checks how much time the actual request may
// probably take, based on the strahler order.
//
function _preRequestUpstream(clickMarker, lon, lat) {
  let subcid = null;
  _preRequest(clickMarker, lon, lat, subcid, _strahlerInformUpstream);
}

function _preRequestUpstreamSubcid(clickMarker, subcid) {
  _preRequest(clickMarker, null, null, subcid, _strahlerInformUpstream);
}

function _preRequestDownstream(clickMarker, lon, lat) {
  let subcid = null;
  _preRequest(clickMarker, lon, lat, subcid, _strahlerInformDownstream);
  // Downstream might be better to use something else, because headwaters
  // exist close to the coast and far from the coast, so strahler is not
  // a good predictor for computation duration...
}

function _preRequestDownstreamSubcid(clickMarker, subcid) {
  _preRequest(clickMarker, null, null, subcid, _strahlerInformDownstream);
}

function _preRequest(clickMarker, lon, lat, subc_id, strahlerInformFunction) {

    // Construct HTTP request to OGC service:
    let xhrPygeo = new XMLHttpRequest();
    var url = "https://aqua.igb-berlin.de/pygeoapi/processes/get-local-streamsegments/execution";
    xhrPygeo.open('POST', url, true)
    xhrPygeo.setRequestHeader('Content-Type', 'application/json');
    xhrPygeo.responseType = 'json';
    // geometry_only must be false, so we get the strahler order returned!
    if (subc_id === null) {
        var payload_inputs_json = JSON.stringify({"inputs":{
          "point": {
            "type": "Point",
            "coordinates": [lon, lat]
          },
          "geometry_only": false
        }});
    } else {
        var payload_inputs_json = JSON.stringify({"inputs":{
          "subc_id": subc_id,
          "geometry_only": false
        }});
    };

    // Define behaviour after response:
    xhrPygeo.onerror = function() {
      console.warn("[pre] Pre-Request: Failed.");
      if (xhrPygeo.status === 405) {
        console.warn("[pre] Pre-Request: Method Not Allowed (405) - likely OPTIONS request failed");
      } else if (xhrPygeo.status === 0) {
        console.warn("[pre] Pre-Request: We got status 0: Request failed or blocked (likely preflight failure)");
      }
    };

    xhrPygeo.onreadystatechange = function () {
      if (xhrPygeo.readyState === XMLHttpRequest.DONE) {
        console.log("[pre] Pre-Request: Done with status:", xhrPygeo.status);
      }
    };

    xhrPygeo.onload = function() {

      // Log response status:
      console.log("[pre] Pre-Request: Returning from OGC process...");
      if (xhrPygeo.status == 200) {
        console.log("[pre] Pre-Request: OGC server returned HTTP 200");
      } else if (xhrPygeo.status == 400) {
        console.warn("[pre] Pre-Request: Oh no: Internal server error (HTTP 400)");
        return
      } else {
        console.warn("[pre] Pre-Request: Oh no: OGC server returned bad HTTP status: "+xhrPygeo.status);
        return
      }

      // Extract strahler order and inform user if a high strahler order was clicked:
      var strahler = xhrPygeo.response.properties.strahler_order;
      strahlerInformFunction(strahler, clickMarker);
    }

    // Send HTTP request:
    console.log('[pre] Pre-Request: Sending HTTP POST request...')
    xhrPygeo.send(payload_inputs_json);
}



// Inform user based on strahler order
function _strahlerInformDownstream(strahler, clickMarker) {
    if (strahler == null) {
      console.warn('[pre] Strahler: No strahler order found...')
    } else if (strahler == 1 | strahler == 2 | strahler == 3) {
      console.log('[pre] Strahler '+strahler+': Uff, will take time...')
      var msg = 'Strahler order '+strahler+', this will take a long time...';
      clickMarker.bindPopup(msg);
    } else if (strahler == 4 | strahler == 5 | strahler == 6) {
      var msg = 'Strahler order '+strahler+', this may take a while...'
      clickMarker.bindPopup(msg);
      console.log('[pre] Strahler '+strahler+': May take a little...')
    } else if (strahler == 7 | strahler == 8 | strahler == 9) {
      console.log('[pre] Strahler '+strahler+': Probably reasonably fast...')
    } else if (strahler >= 10 ) {
      console.log('[pre] Strahler '+strahler+': Probably superfast!')
    } else {
      console.warn('[pre] Strahler: Could not understand strahler order: '+strahler);
    }
}

// Inform user based on strahler order
function _strahlerInformUpstream(strahler, clickMarker) {
    if (strahler == null) {
      console.warn('[pre] Strahler: No strahler order found...')
    } else if (strahler == 1 | strahler == 2 | strahler == 3) {
      console.log('[pre] Strahler '+strahler+': Probably superfast!')
    } else if (strahler == 4 | strahler == 5 | strahler == 6) {
      console.log('[pre] Strahler '+strahler+': May take a little...')
    } else if (strahler == 7 | strahler == 8 | strahler == 9) {
      console.log('[pre] Strahler '+strahler+': May take a while...')
      var msg = 'Strahler order '+strahler+', this may take a while...'
      clickMarker.bindPopup(msg);
    } else if (strahler >= 10 ) {
      console.log('[pre] Strahler '+strahler+': Uff, will take ages...')
      var msg = 'Strahler order '+strahler+', this will take a long time or even fail...';
      clickMarker.bindPopup(msg);
    } else {
      console.warn('[pre] Strahler: Could not understand strahler order: '+strahler);
    }
}


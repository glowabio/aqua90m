

///////////////////////////////////////////////
////// Functions for making OGC requests //////
///////////////////////////////////////////////

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
    _ogcRequest(clickMarker, processId, processDesc, payload_inputs_json) ;
}

// Define making request to OGC service (function):
var ogcRequestOneCoordinatePair = function(map, lon1, lat1, processId, processDesc, logUserAction) {
    console.log('[DEBUG] Triggering suite of actions for one pair of coordinates...');

    // Parse coordinates to Float
    var lon1 = parseFloat(lon1);
    var lat1 = parseFloat(lat1);

    // Add icon and popup to click location:
    clickMarker = putIconToClickLocation(lon1, lat1, map, logUserAction, false, processId, processDesc);
    let paramstring = lon1.toFixed(3)+", "+lat1.toFixed(3)+" (lon, lat)...";
    clickMarker.bindPopup("Waiting for "+processDesc+" for "+paramstring).openPopup();

    // Reset result field:
    document.getElementById("responseField").innerHTML = "Response returned by server for lon, lat <span class=\"code\">"+lon1+", "+lat1+"</span> (lon, lat, WGS84)...";
    document.getElementById("displayGeoJSON").innerHTML = "waiting..."

    // Note: Async function has pre-request here (TODO: Add here too maybe?)

    // Define JSON payload and send:
    var payload_inputs_json = JSON.stringify({"inputs":{
      "point": {
        "type": "Point",
        "coordinates": [lon1, lat1]
      }
    }})
    _ogcRequest(clickMarker, processId, processDesc, payload_inputs_json) ;
}


// Define making request to OGC service (function):
var _ogcRequest = function(clickMarker, processId, processDesc, payload_inputs_json) {
    console.log('[sync] Preparing to make HTTP POST request...')
    document.getElementById("displayGeoJSON").innerHTML = "waiting..."

    // Which pygeoapi instance?
    var url = "https://aqua.igb-berlin.de/pygeoapi/processes/"+processId+"/execution";

    // Construct HTTP request to OGC service:
    let xhrPygeo = new XMLHttpRequest();
    xhrPygeo.open('POST', url, true)
    xhrPygeo.setRequestHeader('Content-Type', 'application/json');
    xhrPygeo.responseType = 'json';

    // Define behaviour for HTTP request:
    // First, error message (e.g. on HTTP 405 / CORS error):
    xhrPygeo.onerror = function() {
      console.error("[sync] Request failed.");
      if (xhrPygeo.status === 405) {
        console.error("[sync] Method Not Allowed (405) - likely OPTIONS request failed");
        clickMarker.bindPopup("HTTP request to service failed (HTTP "+xhrPygeo.status+"). Sorry for that.");
      } else if (xhrPygeo.status === 0) {
        console.error("[sync] We got status 0: Request failed or blocked (likely preflight failure)");
        clickMarker.bindPopup("HTTP request to service failed or blocked, probably a network problem or CORS error. Sorry for that.");
      }
      document.getElementById("displayGeoJSON").innerHTML = "nothing to display"
    };

    xhrPygeo.onreadystatechange = function () {
      if (xhrPygeo.readyState === XMLHttpRequest.DONE) {
        console.log("[sync] Request done with status:", xhrPygeo.status);
      }
    };
    xhrPygeo.onload = function() {
      console.log("[sync] Returning from OGC process: "+processId+"...");
      
      if (xhrPygeo.status == 200) {
        console.error("[sync] OGC server returned HTTP 200");
      } else if (xhrPygeo.status == 400) {
        console.error("[sync] Oh no: Internal server error (HTTP 400)");
        var errmsg = xhrPygeo.response["description"];
        clickMarker.bindPopup(errmsg);
        document.getElementById("displayGeoJSON").innerHTML = errmsg
        return
      } else {
        console.warn("[sync] Oh no: OGC server returned bad HTTP status: "+xhrPygeo.status);
        clickMarker.bindPopup("Failed for unspecified reason (possibly timeout), try another one!!");
        document.getElementById("displayGeoJSON").innerHTML = "nothing to display"
        return
      }

      // If there is no response, it might be a headwater!
      // Stream segments CAN be shown if it is a headwater! Then they would be returned!
      if (xhrPygeo.response == null){
        clickMarker.bindPopup("No "+lookingfor+", is this a headwater?").openPopup();
        // TODO Headwater, how to handle? Has not happened for a while, I think we now include the local
        // one itself to the upstream, so the response will not be null anywhere. Except for ocean I guess.
      } else {
        console.log('[debug] Server response: '+xhrPygeo.response.type);
        console.log('[debug] Server response as JSON: '+JSON.stringify(xhrPygeo.response));
      }

      // Make layer(s) from GeoJSON that the server returned:
      var pygeoResponseGeoJSONLayer = L.geoJSON(xhrPygeo.response);

      // Style features depending on their properties:
      if (document.getElementById("stylingStrahlerToggle").checked){
        pygeoResponseGeoJSONLayer.eachLayer(styleLayerStrahler);
      } else {
        pygeoResponseGeoJSONLayer.eachLayer(styleLayerUni);
      }

      // Add styled layers to map:
      pygeoResponseGeoJSONLayer.addTo(map);
      allMyLayers.push(pygeoResponseGeoJSONLayer);
      console.log('[debug] Added layer to map...');
      map.fitBounds(pygeoResponseGeoJSONLayer.getBounds());
      console.log('[debug] Zoomed to layer...');
      clickMarker.closePopup();

      // Move web page to map!
      document.getElementById("scrollToTop").scrollIntoView();

      // Write GeoJSON into field so that user can copy-paste it:
      var prettyResponse = JSON.stringify(xhrPygeo.response, null, 2); // spacing level = 2
      document.getElementById("displayGeoJSON").innerHTML = prettyResponse;
    };

    // Send HTTP request:
    console.log('[sync] Sending HTTP POST request...')
    xhrPygeo.send(payload_inputs_json);
}
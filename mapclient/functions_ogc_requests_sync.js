

///////////////////////////////////////////////
////// Functions for making OGC requests //////
///////////////////////////////////////////////

// Define making request to OGC service (function):
var ogcRequestTwoCoordinatePairs = function(clickMarker, processId, lon1, lat1, lon2, lat2, processDesc) {

    // Reset result field:
    document.getElementById("responseField").innerHTML = "Response returned by server for <span class=\"code\">"+lon1+", "+lat1+"</span> to <span class=\"code\">"+lon2+", "+lat2+"</span> (lon, lat, WGS84)...";
    document.getElementById("displayGeoJSON").innerHTML = "waiting..."

    // Parse coordinates to Float
    var lat1 = parseFloat(lat1);
    var lon1 = parseFloat(lon1);
    var lat2 = parseFloat(lat2);
    var lon2 = parseFloat(lon2);

    // Param string for logging
    var paramstring = "lat="+lat1.toFixed(3)+", lon="+lon1.toFixed(3)+ "lat="+lat2.toFixed(3)+", lon="+lon2.toFixed(3);

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
    _ogcRequest(clickMarker, processId, processDesc, payload_inputs_json, paramstring) ;
}

// Define making request to OGC service (function):
var ogcRequestOneCoordinatePair = function(clickMarker, processId, lon1, lat1, processDesc) {

    // Reset result field:
    document.getElementById("responseField").innerHTML = "Response returned by server for lon, lat <span class=\"code\">"+lon1+", "+lat1+"</span> (lon, lat, WGS84)...";
    document.getElementById("displayGeoJSON").innerHTML = "waiting..."

    // Parse coordinates to Float
    var lon1 = parseFloat(lon1);
    var lat1 = parseFloat(lat1);

    // Param string for logging
    var paramstring = "lat="+lat1.toFixed(3)+", lon="+lon1.toFixed(3);

    // Define JSON payload and send:
    var payload_inputs_json = JSON.stringify({"inputs":{
      "point": {
        "type": "Point",
        "coordinates": [lon1, lat1]
      }
    }})
    _ogcRequest(clickMarker, processId, processDesc, payload_inputs_json, paramstring) ;
}

// Define making request to OGC service (function):
var _ogcRequest = function(clickMarker, processId, processDesc, payload_inputs_json, paramstring)  {
    console.log('Preparing to make HTTP POST request...')
    document.getElementById("displayGeoJSON").innerHTML = "waiting..."

    // Construct HTTP request to OGC service:
    let xhrPygeo = new XMLHttpRequest();
    var url = "https://aqua.igb-berlin.de/pygeoapi/processes/"+processId+"/execution";
    xhrPygeo.open('POST', url, true)
    xhrPygeo.setRequestHeader('Content-Type', 'application/json');
    xhrPygeo.responseType = 'json';

    // Define behaviour for HTTP request:
    // First, error message (e.g. on HTTP 405 / CORS error):
    xhrPygeo.onerror = function() {
      console.error("Request failed.");
      if (xhrPygeo.status === 405) {
        console.error("Method Not Allowed (405) - likely OPTIONS request failed");
        clickMarker.bindPopup("HTTP request to service failed (HTTP "+xhrPygeo.status+"). Sorry for that.");
      } else if (xhrPygeo.status === 0) {
        console.error("We got status 0: Request failed or blocked (likely preflight failure)");
        clickMarker.bindPopup("HTTP request to service failed or blocked, probably a network problem or CORS error. Sorry for that.");
      }
      document.getElementById("displayGeoJSON").innerHTML = "nothing to display"
    };

    xhrPygeo.onreadystatechange = function () {
      if (xhrPygeo.readyState === XMLHttpRequest.DONE) {
        console.log("Request done with status:", xhrPygeo.status);
      }
    };
    xhrPygeo.onload = function() {
      console.log("Returning from OGC process: "+processId+"...");
      
      if (xhrPygeo.status == 200) {
        console.log("OGC server returned HTTP 200");
        clickMarker.bindPopup("Waiting for "+processDesc+" for "+paramstring+"...").openPopup();
      } else if (xhrPygeo.status == 400) {
        console.log("Oh no: Internal server error (HTTP 400)");
        var errmsg = xhrPygeo.response["description"];
        clickMarker.bindPopup(errmsg);
        document.getElementById("displayGeoJSON").innerHTML = errmsg
        return
      } else {
        console.log("Oh no: OGC server returned bad HTTP status: "+xhrPygeo.status);
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
        console.log('DEBUG: SERVER RESPONSE: '+xhrPygeo.response.type);
        console.log('DEBUG: SERVER RESPONSE AS JSON: '+JSON.stringify(xhrPygeo.response));
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
      console.log('Added layer to map...');
      map.fitBounds(pygeoResponseGeoJSONLayer.getBounds());
      console.log('Zoomed to layer...');
      clickMarker.closePopup();

      // Move web page to map!
      document.getElementById("scrollToTop").scrollIntoView();

      // Write GeoJSON into field so that user can copy-paste it:
      var prettyResponse = JSON.stringify(xhrPygeo.response, null, 2); // spacing level = 2
      document.getElementById("displayGeoJSON").innerHTML = prettyResponse;
    };

    // Send HTTP request:
    console.log('Sending HTTP POST request...')
    xhrPygeo.send(payload_inputs_json);
}
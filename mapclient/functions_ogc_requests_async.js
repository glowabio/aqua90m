

///////////////////////////////////////////////
////// Functions for making OGC requests //////
///////////////////////////////////////////////

// Define making request to OGC service (function):
var ogcRequestTwoCoordinatePairs = function(clickMarker, lon1, lat1, lon2, lat2) {

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
      "lon_start":lon1, "lat_start":lat1,
      "lon_end": lon2, "lat_end": lat2
    }})
    _ogcRequest(clickMarker, payload_inputs_json, paramstring) ;
}

// Define making request to OGC service (function):
var ogcRequestOneCoordinatePair = function(clickMarker, lon1, lat1) {

    // Reset result field:
    document.getElementById("responseField").innerHTML = "Response returned by server for lon, lat <span class=\"code\">"+lon1+", "+lat1+"</span> (lon, lat, WGS84)...";
    document.getElementById("displayGeoJSON").innerHTML = "waiting..."

    // Parse coordinates to Float
    var lon1 = parseFloat(lon1);
    var lat1 = parseFloat(lat1);

    // If upstream, make pre-request:
    var processId = document.getElementById("processes").value;
    if (processId.startsWith("get-upstream")) {
      console.log('Requesting upstream... This may take a while, so we do a pre-request!');
      preRequestUpstream(clickMarker, lon1, lat1);
    }

    // Param string for logging
    var paramstring = "lat="+lat1.toFixed(3)+", lon="+lon1.toFixed(3);

    // Define JSON payload and send:
    var payload_inputs_json = JSON.stringify({"inputs":{"lon":lon1, "lat":lat1}})
    _ogcRequest(clickMarker, payload_inputs_json, paramstring) ;
}


var successPleaseShowGeojson = function(responseJson) {
    console.log("FUNCTION: successPleaseShowGeojson")

    // Make layer(s) from GeoJSON that the server returned:
    var pygeoResponseGeoJSONLayer = L.geoJSON(responseJson);

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
    var prettyResponse = JSON.stringify(responseJson, null, 2); // spacing level = 2
    document.getElementById("displayGeoJSON").innerHTML = prettyResponse;
}

// Pre-Request
function preRequestUpstream(clickMarker, lon, lat) {
  preRequest(clickMarker, lon, lat, strahlerInformUpstream)
}

function preRequest(clickMarker, lon, lat, strahlerInformFunction) {

    // Construct HTTP request to OGC service:
    let xhrPygeo = new XMLHttpRequest();
    var url = "https://aqua.igb-berlin.de/pygeoapi/processes/get-local-streamsegments/execution";
    xhrPygeo.open('POST', url, true)
    xhrPygeo.setRequestHeader('Content-Type', 'application/json');
    xhrPygeo.responseType = 'json';
    // TODO Note: This process expects geometry_only as string, not as bool, which is stupid.
    // Need to change that in pygeoapi, and then here too!
    var payload_inputs_json = JSON.stringify({"inputs":{
      "lon":lon, "lat":lat, 
      "geometry_only": "false"
    }})

    // Define behaviour after response:
    xhrPygeo.onerror = function() {
      console.error("Pre-Request: Failed.");
      if (xhrPygeo.status === 405) {
        console.error("Pre-Request: Method Not Allowed (405) - likely OPTIONS request failed");
      } else if (xhrPygeo.status === 0) {
        console.error("Pre-Request: We got status 0: Request failed or blocked (likely preflight failure)");
      }
    };

    xhrPygeo.onreadystatechange = function () {
      if (xhrPygeo.readyState === XMLHttpRequest.DONE) {
        console.log("Pre-Request: Done with status:", xhrPygeo.status);
      }
    };

    xhrPygeo.onload = function() {

      // Log response status:
      console.log("Pre-Request: Returning from OGC process...");
      if (xhrPygeo.status == 200) {
        console.log("Pre-Request: OGC server returned HTTP 200");
      } else if (xhrPygeo.status == 400) {
        console.log("Pre-Request: Oh no: Internal server error (HTTP 400)");
        return
      } else {
        console.log("Pre-Request: Oh no: OGC server returned bad HTTP status: "+xhrPygeo.status);
        return
      }

      // Extract strahler order and inform user if a high strahler order was clicked:
      var strahler = xhrPygeo.response.properties.strahler_order;
      strahlerInformFunction(strahler, clickMarker);
    }

    // Send HTTP request:
    console.log('Pre-Request: Sending HTTP POST request...')
    xhrPygeo.send(payload_inputs_json);
}

// Inform user based on strahler order
function strahlerInformUpstream(strahler, clickMarker) {
    if (strahler == 1 | strahler == 2 | strahler == 3) {
      console.log('Strahler '+strahler+': Probably superfast!')
    } else if (strahler == 4 | strahler == 5 | strahler == 6) {
      console.log('Strahler '+strahler+': May take a little...')
    } else if (strahler == 7 | strahler == 8 | strahler == 9) {
      console.log('Strahler '+strahler+': May take a while...')
      var msg = 'Strahler order '+strahler+', this may take a while...'
      clickMarker.bindPopup(msg);
    } else {
      console.log('Strahler '+strahler+': Uff, will take ages...')
      var msg = 'Strahler order '+strahler+', this will take a long time or even fail...'
      clickMarker.bindPopup(msg);
    }
}

// Define making request to OGC service (function):
//var _ogcRequest = function(clickMarker, payload_inputs_json, paramstring)  {
async function _ogcRequest(clickMarker, payload_inputs_json, paramstring)  {
    console.log('Preparing to make HTTP POST request...')
    document.getElementById("displayGeoJSON").innerHTML = "waiting..."

    // Which process?
    var dropdown = document.getElementById("processes");
    var processId = dropdown.value;
    var processDesc = dropdown.options[dropdown.selectedIndex].text;

    // Which pygeoapi instance?
    var url = "https://aqua.igb-berlin.de/pygeoapi/processes/"+processId+"/execution";
    
    try {

      // Make the initial request for processing:
      console.log('Sending HTTP POST request asynchronously...')
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Prefer': 'respond-async' },
        body: payload_inputs_json
      });
      console.log('HTTP Status Code:', response.status);

      // Retrieve the body:
      let responseBody;
      try {
        responseBody = await response.json();
      } catch (e) {
        console.error('Failed to parse JSON:', e);
        responseBody = null;
      }

      // If the initial request fails, inform the user and throw an error:
      if (!response.ok) {
        console.error("Request failed.");
        if (response.status == 400) {
            console.log("Oh no: Internal server error (HTTP 400)");
            var errmsg = responseBody.description;
            clickMarker.bindPopup(errmsg);
            document.getElementById("displayGeoJSON").innerHTML = errmsg
        } else if (response.status == 405) {
            console.error("Method Not Allowed (405) - likely OPTIONS request failed");
            document.getElementById("displayGeoJSON").innerHTML = "nothing to display"
            clickMarker.bindPopup("HTTP request to service failed (HTTP 405). Sorry for that.");
        } else if (response.status === 0) {
            console.error("We got status 0: Request failed or blocked (likely preflight failure)");
            document.getElementById("displayGeoJSON").innerHTML = "nothing to display"
            clickMarker.bindPopup("HTTP request to service failed or blocked, probably a network problem or CORS error. Sorry for that.");
        } else {
            console.log("Oh no: OGC server returned bad HTTP status: "+response.status);
            clickMarker.bindPopup("Failed for unspecified reason (possibly timeout), try another one!!");
            document.getElementById("displayGeoJSON").innerHTML = "nothing to display"
        }
        throw new Error('Failed to start process');
      }

      // Get the URL where to poll for the response:
      const statusUrl = response.headers.get('Location');
      if (!statusUrl) {
        throw new Error('No Location header returned');
      }

      // Poll for the status...
      console.log("Status: processing...");
      clickMarker.bindPopup("Waiting for "+processDesc+" for "+paramstring+"...").openPopup();
      pollStatus(statusUrl, processId, clickMarker);

    // What kind of errors could this be?
    // Something that prevented even the proper HTTP request, I guess...
    } catch (error) {
      var errmsg = `Error: ${error.message}`;
      console.log(errmsg);
      document.getElementById("displayGeoJSON").innerHTML = errmsg
    }
}

async function pollStatus(statusUrl, processId, clickMarker) {
    const delay = ms => new Promise(res => setTimeout(res, ms));

    while (true) {
      await delay(2000);

      const res = await fetch(statusUrl);
      const job = await res.json();

      //document.getElementById('status').textContent = `Status: ${job.status}`;
      console.log(`Status: ${job.status}`);
      document.getElementById("displayGeoJSON").innerHTML = `Status: ${job.status}`;

      // If successful, find the "application/json" result link
      if (job.status === 'successful') {
        const jsonResultLink = job.links?.find(
          link =>
            link.rel === "http://www.opengis.net/def/rel/ogc/1.0/results" &&
            link.type === "application/json"
        );

        // If we cannot find a result link:
        if (!jsonResultLink) {
          console.warn("Job succeeded, but no JSON result link was found.");
          // TODO Maybe throw error here.
          break;
        }

        // If we do find a result link, fetch the result:
        try {
          const resultRes = await fetch(jsonResultLink.href);
          console.log("Returning from OGC process: "+processId+"...");

          // If the result fetching failed:
          if (!resultRes.ok) {
            if (resultRes.status == 400) {
                console.log("Oh no: Internal server error (HTTP 400)");
                const resultData = await resultRes.json();
                var errmsg = resultData.description;
                clickMarker.bindPopup(errmsg);
                document.getElementById("displayGeoJSON").innerHTML = errmsg;
            } else {
                console.log("Oh no: OGC server returned bad HTTP status: "+resultRes.status);
                clickMarker.bindPopup("Failed for unspecified reason (possibly timeout), try another one!!");
                document.getElementById("displayGeoJSON").innerHTML = "nothing to display";
            }
            throw new Error("Failed to fetch results.");
          }

          // If the result fetching succeeded, get the JSON from the response:
          const resultData = await resultRes.json();
          // Display raw JSON for now - later the pretty JSON will be shown!
          document.getElementById("displayGeoJSON").innerHTML = JSON.stringify(resultData, null, 2);

          // If there is no response, it might be a headwater!
          // TODO: Is this in the right location? Wouldn't the resultRes.json() have thrown an error anyway?
          // Stream segments CAN be shown if it is a headwater! Then they would be returned!
          if (resultData == null){
            console.error('Result data is null... This should not happen.')
            clickMarker.bindPopup("No "+lookingfor+", is this a headwater?").openPopup();
            // TODO Headwater, how to handle? Has not happened for a while, I think we now include the local
            // one itself to the upstream, so the response will not be null anywhere. Except for ocean I guess.
          }

          // Check if it's valid GeoJSON
          if (resultData.type && (
              resultData.type === "FeatureCollection" ||
              resultData.type === "Feature" ||
              resultData.type === "GeometryCollection")
          ) {

            // Now: We received a response, and it's valid GeoJSON!
            // So here we start the whole display stuff!
            successPleaseShowGeojson(resultData);

          } else {
            console.warn("Result JSON is not valid GeoJSON.");
          }

        // If the response is not valid GeoJSON:
        } catch (err) {
            var errmsg = "Job succeeded, but result fetch failed: " + err.message;
            console.warn(errmsg);
            document.getElementById("displayGeoJSON").innerHTML = errmsg;
            clickMarker.bindPopup(errmsg);
        }
        break;

      // If the job has failed or was dismissed:
      } else if (['failed', 'dismissed'].includes(job.status)) {
        var errmsg = `Job ${job.status}.`;
        console.warn(errmsg);
        document.getElementById('displayGeoJSON').innerHTML = errmsg;
        clickMarker.bindPopup(errmsg);
        break;
      }
    }
}


///////////////////////////////////////////////
////// Functions for making OGC requests //////
///////////////////////////////////////////////

var _successPleaseShowGeojson = function(responseJson, processId) {
    console.log("Displaying GeoJSON on the map...");

    // Make layer(s) from GeoJSON that the server returned:
    var pygeoResponseGeoJSONLayer = L.geoJSON(responseJson);

    // Style features depending on their properties:
    if (document.getElementById("stylingStrahlerToggle").checked){
        console.log("[DEBUG] Asked to style depending on strahler order.");
        pygeoResponseGeoJSONLayer.eachLayer(function(layer) {
            styleLayerStrahler(layer, processId);
        });
    } else {
        console.log("[DEBUG] Will style without strahler order.");
        pygeoResponseGeoJSONLayer.eachLayer(function(layer) {
            styleLayerUni(layer, processId);
        });
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


// Define making request to OGC service (function):
async function _ogcRequest(server, processId, processDesc, payload_inputs_json, clickMarker) {
    console.log('[async] Preparing to make HTTP POST request...')
    document.getElementById("displayGeoJSON").innerHTML = "waiting..."

    // Define pygeoapi endpoint:
    var url = server+"/processes/"+processId+"/execution";

    try {

      // Make the initial request for processing:
      console.log('[async] Sending HTTP POST request asynchronously...')
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Prefer': 'respond-async' },
        body: payload_inputs_json
      });
      console.log('[async] HTTP Status Code:', response.status);

      // Retrieve the body:
      let responseBody;
      try {
        responseBody = await response.json();
      } catch (e) {
        console.error('[async] Failed to parse JSON:', e);
        responseBody = null;
      }

      // If the initial request fails, inform the user and throw an error:
      if (!response.ok) {
        console.error("[async] Request failed.");
        if (response.status == 400) {
            console.warn("[async] Oh no: Internal server error (HTTP 400)");
            var errmsg = responseBody.description;
            clickMarker.bindPopup(errmsg);
            document.getElementById("displayGeoJSON").innerHTML = errmsg
        } else if (response.status == 405) {
            console.warn("[async] Method Not Allowed (405) - likely OPTIONS request failed");
            document.getElementById("displayGeoJSON").innerHTML = "nothing to display"
            clickMarker.bindPopup("HTTP request to service failed (HTTP 405). Sorry for that.");
        } else if (response.status === 0) {
            console.warn("[async] We got status 0: Request failed or blocked (likely preflight failure)");
            document.getElementById("displayGeoJSON").innerHTML = "nothing to display"
            clickMarker.bindPopup("HTTP request to service failed or blocked, probably a network problem or CORS error. Sorry for that.");
        } else {
            console.warn("[async] Oh no: OGC server returned bad HTTP status: "+response.status);
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
      _pollStatus(statusUrl, processId, clickMarker);

    // What kind of errors could this be?
    // Something that prevented even the proper HTTP request, I guess...
    } catch (error) {
      var errmsg = `Error: ${error.message}`;
      console.warn('[async] '+errmsg);
      document.getElementById("displayGeoJSON").innerHTML = errmsg
    }
}

async function _pollStatus(statusUrl, processId, clickMarker) {
    const delay = ms => new Promise(res => setTimeout(res, ms));
    const startTime = performance.now(); // high-resolution timer

    while (true) {
      await delay(2000);

      const elapsedMs = performance.now() - startTime;
      const elapsedSec = (elapsedMs / 1000).toFixed(1);
      console.log(`[async] ${elapsedSec}s since first request`);

      const res = await fetch(statusUrl);
      if (!res.ok) {
        console.error(`[async] HTTP error: ${res.status} ${res.statusText}`);
        continue;
      }

      const job = await res.json();

      //document.getElementById('status').textContent = `Status: ${job.status}`;
      console.log(`[async] Status: ${job.status}`);
      document.getElementById("displayGeoJSON").innerHTML = `Status: ${job.status}: ${elapsedSec}s since first request`;

      if (['accepted', 'running'].includes(job.status)) {
         //clickMarker.bindPopup(`Status: ${job.status}: ${elapsedSec}s since first request`);
	 const popup = clickMarker.getPopup();
         let content = popup.getContent();
         const updated = content.replace(
           /(waited:\s*)[\d.]+/,
           `$1${elapsedSec}`
         );

         // Update popup content
         console.log("[DEBUG] Updated popup content: "+updated);
         popup.setContent(updated);

         // If popup is open, refresh it visually
         clickMarker.setPopupContent(updated);

      // If successful, find the "application/json" result link
      } else if (job.status === 'successful') {
        const jsonResultLink = job.links?.find(
          link =>
            link.rel === "http://www.opengis.net/def/rel/ogc/1.0/results" &&
            link.type === "application/json"
        );

        // If we cannot find a result link:
        if (!jsonResultLink) {
          console.warn("[async] Job succeeded, but no JSON result link was found.");
          // TODO Maybe throw error here.
          break;
        }

        // If we do find a result link, fetch the result:
        try {
          const resultRes = await fetch(jsonResultLink.href);
          console.log("[async] Returning from OGC process: "+processId+"...");

          // If the result fetching failed:
          if (!resultRes.ok) {
            if (resultRes.status == 400) {
                console.warn("[async] Oh no: Internal server error (HTTP 400)");
                const resultData = await resultRes.json();
                var errmsg = resultData.description;
                clickMarker.bindPopup(errmsg);
                document.getElementById("displayGeoJSON").innerHTML = errmsg;
            } else {
                console.warn("[async] Oh no: OGC server returned bad HTTP status: "+resultRes.status);
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
            console.warn('[async] Result data is null... This should not happen.')
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
            _successPleaseShowGeojson(resultData, processId);

          } else {
            console.warn("[async] Result JSON is not valid GeoJSON.");
          }

        // If the response is not valid GeoJSON:
        } catch (err) {
            var errmsg = "Job succeeded, but result fetch failed: " + err.message;
            console.warn('[async] '+errmsg);
            document.getElementById("displayGeoJSON").innerHTML = errmsg;
            clickMarker.bindPopup(errmsg);
        }
        break;

      // If the job has failed or was dismissed:
      } else if (['failed', 'dismissed'].includes(job.status)) {
        if (job.description) {
          var errmsg = `Job ${job.status}: ${job.description}`;
        } else if (job.message) {
          var errmsg = `Job ${job.status}: ${job.message}`;
        } else {
          var errmsg = `Job ${job.status}.`;
        }
        console.warn('[async] '+errmsg);
        document.getElementById('displayGeoJSON').innerHTML = errmsg;
        clickMarker.bindPopup(errmsg);
        break;
      }
    }
}

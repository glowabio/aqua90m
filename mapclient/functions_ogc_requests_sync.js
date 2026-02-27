///////////////////////////////////////////////
////// Functions for making OGC requests //////
///////////////////////////////////////////////

// Define making request to OGC service (function):
var _ogcRequest = function(server, processId, processDesc, payload_inputs_json, clickMarker) {
    console.log('[sync] Preparing to make HTTP POST request...')
    document.getElementById("displayGeoJSON").innerHTML = "waiting..."

    // Define pygeoapi endpoint:
    var url = server+"/processes/"+processId+"/execution";

    // Construct HTTP request to OGC service:
    let xhrPygeo = new XMLHttpRequest();
    const startTime = performance.now(); // high-resolution timer
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
        const elapsedMs = performance.now() - startTime;
        const elapsedSec = (elapsedMs / 1000).toFixed(1);
        console.log(`[sync] ${elapsedSec}s since first request`);
        // update click marker. NOT TESTED YET! TODO: Test
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
      }
    };
    xhrPygeo.onload = function() {
      console.log("[sync] Returning from OGC process: "+processId+"...");
      
      if (xhrPygeo.status == 200) {
        console.log("[sync] OGC server returned HTTP 200");
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
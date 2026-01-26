////////////////////////////////////
////// Define button behaviour /////
////////////////////////////////////

// This file contains one function: What happens if a user clicks the button
// that indicates that they entered custom values in the form and want those
// to be used.


// Define behaviour for custom lonlat button (function):
// Button clicked by user after entering their own coordinates (one or two pairs)
var customButtonClickBehaviour = function() {
  console.log("User requested to use their own values...")

  // Get first (and only?) pair of coordinates:
  var lon1 = document.getElementById("customLon1").value;
  var lat1 = document.getElementById("customLat1").value;

  // Should we use subc_id instead of coordinates?
  var subcid1 = document.getElementById("customSubc1").value;
  var use_subcid1 = false;
  if (subcid1 === "") {
    console.log("Found no subcid1: "+ subcid1);
  } else {
    console.log("Found a subcid1: "+subcid1);
    use_subcid1 = true;
    subcid1 = parseInt(subcid1);
  }

  // Which process? Does it need one or two input coordinate pairs?
  var dropdown = document.getElementById("processes");
  let processId = dropdown.value;
  let processDesc = processId; // TODO: Find proper process description!
  let pairs = dropdown.options[dropdown.selectedIndex].dataset.pairs;
  if (pairs == "one") {

    // User entered coordinate pair:
    if (!use_subcid1) {
      console.log("Clicked button for one coordinate pair: "+lon1+", "+lat1+" (lon, lat, WGS84).");
      let logUserAction = "entered a coordinate pair";
      ogcRequestOneCoordinatePair(map, lon1, lat1, processId, processDesc, logUserAction);

    } else {

      // If snapping, using subc_id cannot work! Need coordinates to snap!
      if (processId.includes("snapped")) {
        var errmsg = "Cannot run process "+processDesc+" for a subcatchment id, need coordinates!";
        console.warn("Oh no: "+errmsg);
        let logUserAction = "entered a subc_id for snapping (which cannot work)";
        clickMarker = putIconToSubcidLocation(map, logUserAction);
        clickMarker.bindPopup(errmsg);
        alert(errmsg);
        throw new Error(errmsg); // So that no OGC request is attempted
      }

      console.log("Clicked button for one subcid "+subcid1+".");
      let logUserAction = "entered a subc_id (location unknown)";
      // We have no click location...
      clickMarker = putIconToSubcidLocation(map, logUserAction);
      clickMarker.bindPopup("Waiting for "+processDesc+" for two subcatchments...").openPopup();
      // Reset result field:
      document.getElementById("responseField").innerHTML = "Response returned by server for subc_id <span class=\"code\">"+subcid1+"</span>...";
      document.getElementById("displayGeoJSON").innerHTML = "waiting..."
      // Construct and send HTTP request to OGC service:
      // Param string for logging
      var logstring = "subcid="+subcid1;
      // Define JSON payload and send:
      var payload_inputs_json = JSON.stringify({"inputs":{"subc_id":subcid1}})
      _ogcRequest(clickMarker, processId, payload_inputs_json, logstring, processDesc) ;
    }

  } else if (pairs == "two") {

    // Get second pair of coordinates:
    var lon2 = document.getElementById("customLon2").value;
    var lat2 = document.getElementById("customLat2").value;

    // Should we use subc_id instead of coordinates?
    var subcid2 = document.getElementById("customSubc2").value;
    var use_subcid2 = false;
    if (subcid2 === "") {
      console.log("Found no subcid2: "+ subcid2);
    } else {
      console.log("Found a subcid2: "+subcid2);
      use_subcid2 = true;
      subcid2 = parseInt(subcid2);
    }

    // Two coordinates:
    if (!use_subcid1 && !use_subcid2) {
      console.log("Clicked button for two coordinate pairs: "+lon1+", "+lat1+" and "+lon2+", "+lat2+" (lon, lat, WGS84)");
      let logUserAction = "entered two coordinate pairs";
      ogcRequestTwoCoordinatePairs(map, lon1, lat1, lon2, lat2, processId, processDesc, logUserAction);

    // Two subcids:
    } else if (use_subcid1 && use_subcid2) {
      console.log("Clicked button for two subcids: "+subcid1+", "+subcid2+".");
      // We have no click location... Putting it to a corner of the map, but it will stay at that map location...
      // TODO: Can we later move the clickmarker somewhere?
      let northEast = map.getBounds().getNorthEast();
      let lat = northEast.lat;
      let lon = northEast.lng;
      console.log('Map Northeast: '+lon+', '+lat)
      clickMarker = putIconToClickLocation(lon, lat, map, "entered two subc_ids (location unknown)");
      clickMarker.bindPopup("Waiting for "+processDesc+" for two subcatchments").openPopup();

      // Reset result field:
      document.getElementById("responseField").innerHTML = "Response returned by server for subc_id <span class=\"code\">"+subcid1+"</span> to <span class=\"code\">"+subcid2+"...";
      document.getElementById("displayGeoJSON").innerHTML = "waiting..."
      // Construct and send HTTP request to OGC service:
      // Param string for logging
      var logstring = "subcid1="+subcid1+",subcid2="+subcid2;
      // Define JSON payload and send:
      var payload_inputs_json = JSON.stringify({"inputs":{"subc_id_start":subcid1, "subc_id_end": subcid2}})
      _ogcRequest(clickMarker, processId, processDesc, payload_inputs_json, logstring);

    // Mixed 1
    } else if (use_subcid1 && !use_subcid2) {
      // Param string for logging
      var logstring = "subcid1="+subcid1+",lon2="+lon2+",lat2="+lat2;
      console.log("Clicked button for mixed: "+logstring+".");

      // We have no click location...
      // WE NEED TWO CLICKMARKERS??? WIP
      // Use known location for click marker (TODO IMPROVE)
      clickMarker = putIconToClickLocation(lon2, lat2, map, "entered two subc_ids (location unknown)");
      clickMarker.bindPopup("Waiting for "+processDesc+" for subcatchment and coordinate").openPopup();

      // Reset result field:
      document.getElementById("responseField").innerHTML = "Response returned by server for subc_id <span class=\"code\">"+subcid1+"</span> to <span class=\"code\">"+lon2+", "+lat2+"</span> (lon, lat, WGS84)...";
      document.getElementById("displayGeoJSON").innerHTML = "waiting..."

      // Construct and send HTTP request to OGC service:
      // Define JSON payload and send:
      var payload_inputs_json = JSON.stringify({"inputs":{"subc_id_start":subcid1, "lon_end": lon2, "lat_end": lat2}})
      _ogcRequest(clickMarker, processId, processDesc, payload_inputs_json, logstring);

    // Mixed 2:
    } else if (!use_subcid1 && use_subcid2) {
      var logstring = "lon1="+lon1+",lat1="+lat1+",subcid2="+subcid2;
      console.log("Clicked button for mixed: "+logstring+".");

      // We have no click location...
      // Use known location for click marker (TODO IMPROVE)
      clickMarker = putIconToClickLocation(lon1, lat1, map, "entered mixed "+logstring);
      clickMarker.bindPopup("Waiting for "+processDesc+" for subcatchment and coordinate").openPopup();
      // Reset result field:
      document.getElementById("responseField").innerHTML = "Response returned by server for <span class=\"code\">"+lon1+", "+lat1+"</span> (lon, lat, WGS84) to subc_id <span class=\"code\">"+subcid2+"</span>...";
      document.getElementById("displayGeoJSON").innerHTML = "waiting..."

      // Construct and send HTTP request to OGC service:
      // Define JSON payload and send:
      var payload_inputs_json = JSON.stringify({"inputs":{"lon_start": lon1, "lat_start": lat1, "subc_id_end":subcid2}})
      _ogcRequest(clickMarker, processId, processDesc, payload_inputs_json, logstring);
    }
  }
}

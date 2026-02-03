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

  // Scroll to top
  document.getElementById("scrollToTop").scrollIntoView();

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

  // If it needs just one:
  if (pairs == "one") {

    // User entered coordinate pair:
    if (!use_subcid1) {
      console.log("Clicked button for one coordinate pair: "+lon1+", "+lat1+" (lon, lat, WGS84).");
      let logUserAction = "entered a coordinate pair";
      ogcRequestOneCoordinatePair(map, lon1, lat1, processId, processDesc, logUserAction);

    // User entered subc_id:
    } else {
      console.log("Clicked button for one subcid "+subcid1+".");

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

      let logUserAction = "entered a subc_id (location unknown)";
      ogcRequestOneSubcid(map, subcid1, processId, processDesc, logUserAction);
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
      console.log("Clicked button for two subc_ids: "+subcid1+" and "+subcid2+".");
      let logUserAction = "entered two subc_ids (location unknown)";
      ogcRequestTwoSubcids(map, subcid1, subcid2, processId, processDesc, logUserAction);

    // Mixed 1
    // Note: Whether the user entered subc_id in the left or right field, does not matter;
    // the exact same request is sent in both "mixed" cases. Should the order matter at some
    // point, add another "ogcRequestTwoMixed()" function.
    } else if (use_subcid1 && !use_subcid2) {
      console.log("Clicked button for mixed: subcid1="+subcid1+",lon2="+lon2+",lat2="+lat2+".");
      let logUserAction = "entered a coordinate pair and a subc_id";
      ogcRequestTwoMixed(map, lon2, lat2, subcid1, processId, processDesc, logUserAction);

    // Mixed 2:
    // Note: Whether the user entered subc_id in the left or right field, does not matter;
    // the exact same request is sent in both "mixed" cases. Should the order matter at some
    // point, add another "ogcRequestTwoMixed()" function.
    } else if (!use_subcid1 && use_subcid2) {
      console.log("Clicked button for mixed: lon1="+lon1+",lat1="+lat1+",subcid2="+subcid2+".");
      let logUserAction = "entered a coordinate pair and a subc_id";
      ogcRequestTwoMixed(map, lon1, lat1, subcid2, processId, processDesc, logUserAction);
    }
  }
}




    ////////////////////////////////////
    ////// Define button behaviour /////
    ////////////////////////////////////

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
      let pairs = dropdown.options[dropdown.selectedIndex].dataset.pairs;
      if (pairs == "one") {

        if (!use_subcid1) {
          console.log("Clicked button for one coordinate pair: "+lon1+", "+lat1+" (lon, lat, WGS84)");
          clickMarker = putIconToClickLocation(lon1, lat1, map, "clicked on button", false);
          document.getElementById("scrollToTop").scrollIntoView();
          // Construct and send HTTP request to OGC service:
          ogcRequestOneCoordinatePair(clickMarker, lon1, lat1);

        } else {
          console.log("Clicked button for one subcid "+subcid1+".");
          // We have no click location... Putting it to a corner of the map, but it will stay at that map location...
          // TODO: Can we later move the clickmarker somewhere?
          //let northEast = map.getBounds().getNorthEast();
          //console.log("map getBounds: "+map.getBounds());
          //let lat = northEast.lat;
          //let lon = northEast.lng;
          //console.log('Map Northeast: '+lon+', '+lat);
          //clickMarker = putIconToClickLocation(lon, lat, map, "entered a subc_id (location unknown)", false);
          clickMarker = putIconToClickLocation(null, null, map, "entered a subc_id (location unknown)", false);
          // Reset result field:
          document.getElementById("responseField").innerHTML = "Response returned by server for subc_id <span class=\"code\">"+subcid1+"</span>...";
          document.getElementById("displayGeoJSON").innerHTML = "waiting..."
          // Construct and send HTTP request to OGC service:
          // Param string for logging
          var logstring = "subcid="+subcid1;
          // Define JSON payload and send:
          var payload_inputs_json = JSON.stringify({"inputs":{"subc_id":subcid1}})
          _ogcRequest(clickMarker, payload_inputs_json, logstring) ;
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
          //buttonClickBehaviourTwoPairs(lon1, lat1, lon2, lat2); WIPPP
          console.log("Clicked button for two coordinate pairs: "+lon1+", "+lat1+" and "+lon2+", "+lat2+" (lon, lat, WGS84)");
          // Add icon and popup to click location:
          clickMarker = putIconToClickLocation(lon1, lat1, map, "clicked on button (part 1)", false);
          clickMarker = putIconToClickLocation(lon2, lat2, map, "clicked on button (part 2)", false);
          document.getElementById("scrollToTop").scrollIntoView();
          // Construct and send HTTP request to OGC service:
          ogcRequestTwoCoordinatePairs(clickMarker, lon1, lat1, lon2, lat2);

        // Two subcids:
        } else if (use_subcid1 && use_subcid2) {
          console.log("Clicked button for two subcids: "+subcid1+", "+subcid2+".");
          // We have no click location... Putting it to a corner of the map, but it will stay at that map location...
          // TODO: Can we later move the clickmarker somewhere?
          let northEast = map.getBounds().getNorthEast();
          let lat = northEast.lat;
          let lon = northEast.lng;
          console.log('Map Northeast: '+lon+', '+lat)
          clickMarker = putIconToClickLocation(lon, lat, map, "entered two subc_ids (location unknown)", false);
          // Reset result field:
          document.getElementById("responseField").innerHTML = "Response returned by server for subc_id <span class=\"code\">"+subcid1+"</span> to <span class=\"code\">"+subcid2+"...";
          document.getElementById("displayGeoJSON").innerHTML = "waiting..."
          // Construct and send HTTP request to OGC service:
          // Param string for logging
          var logstring = "subcid1="+subcid1+",subcid2="+subcid2;
          // Define JSON payload and send:
          var payload_inputs_json = JSON.stringify({"inputs":{"subc_id_start":subcid1, "subc_id_end": subcid2}})
          _ogcRequest(clickMarker, payload_inputs_json, logstring);

        // Mixed 1
        } else if (use_subcid1 && !use_subcid2) { WIP
          // Param string for logging
          var logstring = "subcid1="+subcid1+",lon2="+lon2+",lat2="+lat2;
          console.log("Clicked button for mixed: "+logstring+".");

          // We have no click location... Putting it to a corner of the map, but it will stay at that map location...
          // TODO: Can we later move the clickmarker somewhere?
          // WE NEED TWO CLICKMARKERS??? WIP
          //let northEast = map.getBounds().getNorthEast();
          //let lat = northEast.lat;
          //let lon = northEast.lng;
          //console.log('Map Northeast: '+lon+', '+lat)

          // Use known location for click marker (TODO IMPROVE)
          clickMarker = putIconToClickLocation(lon2, lat2, map, "entered two subc_ids (location unknown)", false);

          // Reset result field:
          document.getElementById("responseField").innerHTML = "Response returned by server for subc_id <span class=\"code\">"+subcid1+"</span> to <span class=\"code\">"+lon2+", "+lat2+"</span> (lon, lat, WGS84)...";
          document.getElementById("displayGeoJSON").innerHTML = "waiting..."

          // Construct and send HTTP request to OGC service:
          // Define JSON payload and send:
          var payload_inputs_json = JSON.stringify({"inputs":{"subc_id_start":subcid1, "lon_end": lon2, "lat_end": lat2}})
          _ogcRequest(clickMarker, payload_inputs_json, logstring);

        // Mixed 2:
        } else if (!use_subcid1 && use_subcid2) {
          var logstring = "lon1="+lon1+",lat1="+lat1+",subcid2="+subcid2;
          console.log("Clicked button for mixed: "+logstring+".");

          // We have no click location... Putting it to a corner of the map, but it will stay at that map location...
          // TODO: Can we later move the clickmarker somewhere?
          // WE NEED TWO CLICKMARKERS??? WIP
          //let northEast = map.getBounds().getNorthEast();
          //let lat = northEast.lat;
          //let lon = northEast.lng;
          //console.log('Map Northeast: '+lon+', '+lat)
          // Param string for logging

          // Use known location for click marker (TODO IMPROVE)
          clickMarker = putIconToClickLocation(lon1, lat1, map, "entered mixed "+logstring, false);

          // Reset result field:
          document.getElementById("responseField").innerHTML = "Response returned by server for <span class=\"code\">"+lon1+", "+lat1+"</span> (lon, lat, WGS84) to subc_id <span class=\"code\">"+subcid2+"</span>...";
          document.getElementById("displayGeoJSON").innerHTML = "waiting..."

          // Construct and send HTTP request to OGC service:
          // Define JSON payload and send:
          var payload_inputs_json = JSON.stringify({"inputs":{"lon_start": lon1, "lat_start": lat1, "subc_id_end":subcid2}})
          _ogcRequest(clickMarker, payload_inputs_json, logstring);
        }
      }
    }

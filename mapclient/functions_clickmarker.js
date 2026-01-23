///////////////////////////
/// Click marker on map ///
///////////////////////////


// Function to add a grey marker, and a pop-up, to a location on the map
var putIconToClickLocation = function(lon1, lat1, map, actionDone, askToClickAgain, processId, processDesc) {
  console.log("[icon] Creating icon at coordinates: "+lon1+", "+lat1+" (lon, lat, WGS84), because user "+actionDone+".");

  // Define icon size
  var iconheight = 30;
  var iconwidth = 0.61*iconheight;

  // Where to place the icon
  if (lon1 === null) {
    // Using map center to place the popup, but we don't use the map center as
    // processing input coordinates!
    var latlon = map.getCenter();
    console.log('[icon] Using map centre for placing popup: '+latlon)
    // This string will be displayed until function _ogcRequest() updates the text:
    var paramstring = "[no coordinates to display]";
    var iconUrl = 'data:image/gif;base64,R0lGODlhAQABAAD/ACwAAAAAAQABAAACADs='; // 1x1 transparent gif
    var iconSize = [1, 1]; // minimal size
  } else {
    var lon1 = parseFloat(lon1);
    var lat1 = parseFloat(lat1);
    var latlon = [lat1, lon1];
    var paramstring = lon1.toFixed(3)+", "+lat1.toFixed(3)+" (lon, lat)...";
    var iconSize = [iconwidth, iconheight];
    var iconUrl = 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-grey.png';
  }

  // Icon for user-clicks
  var clickIcon = L.icon({
    iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-grey.png',
    iconSize: iconSize,
    iconAnchor: [iconwidth*0.5, iconheight], // from top left corner: go half-width to right, full-height to bottom 
    popupAnchor: [0, -iconheight-5] // where iconAnchor is, go full-height to top plus 5 pixels
  });
  let clickMarker = L.marker(latlon, {icon: clickIcon}).addTo(map);
  allMyIcons.push(clickMarker);

  // Add Popup to user-click
  //console.log("Icon: When "+actionDone+", this process was selected: "+processId+"...");
  if (askToClickAgain) {
    clickMarker.bindPopup("You selected "+processDesc+", so please click another time!").openPopup();
  } else {
    // Not a good place for defining this text, because here we only know the stuff relevant for
    // placing the popup (we don't know the subc_id etc.)...
    clickMarker.bindPopup("Waiting for "+processDesc+" for "+paramstring).openPopup();
    // This popup will be redefined/overwritten in ogcRequestTwoCoordinatePairs(), so put the same text!
  }
  return (clickMarker);
}
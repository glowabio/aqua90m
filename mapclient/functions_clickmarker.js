///////////////////////////
/// Click marker on map ///
///////////////////////////

// Function to add a grey marker to a location on the map
var putIconToClickLocation = function(lon1, lat1, map, logUserAction) {
  console.log("[icon] Creating icon at coordinates: "+lon1+", "+lat1+" (lon, lat, WGS84), because user "+actionDone+".");

  // Where to place the icon
  var lon1 = parseFloat(lon1);
  var lat1 = parseFloat(lat1);

  // Define icon
  var iconSize = [iconwidth, iconheight];
  var iconUrl = 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-grey.png';

  var clickMarker = _putIconToLocation = function([lat1, lon1], map, iconUrl, iconSize);
  return (clickMarker);
}


// Function to add a transparent marker to a location on the map
var putIconToSubcidLocation = function(map, logUserAction) {
  console.log("[icon] Creating icon, location to be determined, because user "+actionDone+".");

  // Where to place the icon?

  // We have no click location... Putting it to a corner of the map, but it will stay at that map location...
  // TODO: Can we later move the clickmarker somewhere?
  //let northEast = map.getBounds().getNorthEast();
  //console.log("map getBounds: "+map.getBounds());
  //let lat = northEast.lat;
  //let lon = northEast.lng;
  //console.log('Map Northeast: '+lon+', '+lat);

  // Using map center to place the popup, but we don't use the map center as
  // processing input coordinates!
  var latlon = map.getCenter();
  console.log('[icon] Using map centre for placing popup: '+latlon)

  // Define icon size
  var iconSize = [1, 1]; // minimal size
  var iconUrl = 'data:image/gif;base64,R0lGODlhAQABAAD/ACwAAAAAAQABAAACADs='; // 1x1 transparent gif

  var clickMarker = _putIconToLocation = function([lat1, lon1], map, iconUrl, iconSize, logUserAction);
  return (clickMarker);
}


// Function to add a marker to a location on the map
var _putIconToLocation = function(latlon, map, iconUrl, iconSize, logUserAction) {
  console.log("[icon] Creating icon at place: "+latlon+", because user "+logUserAction+".");

  // Define icon
  var iconheight = 30;
  var iconwidth = 0.61*iconheight;
  var clickIcon = L.icon({
    iconUrl: iconUrl,
    iconSize: iconSize,
    iconAnchor: [iconwidth*0.5, iconheight], // from top left corner: go half-width to right, full-height to bottom 
    popupAnchor: [0, -iconheight-5] // where iconAnchor is, go full-height to top plus 5 pixels
  });

  // Add icon to map
  let clickMarker = L.marker(latlon, {icon: clickIcon}).addTo(map);

  // Keep icon for further use
  allMyIcons.push(clickMarker);

  return (clickMarker);
}

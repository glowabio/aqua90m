///////////////////////////
/// Click marker on map ///
///////////////////////////

// Function to add a grey marker to a location on the map
var putIconToClickLocation = function(lon1, lat1, map, logUserAction) {
  console.log("[icon] Creating icon at coordinates: "+lon1+", "+lat1+" (lon, lat, WGS84), because user "+logUserAction+".");

  // Where to place the icon
  var lon1 = parseFloat(lon1);
  var lat1 = parseFloat(lat1);

  // Define icon
  var iconHeight = 30;
  var iconWidth = 0.61*iconHeight;
  var iconUrl = 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-grey.png';

  var clickMarker = _putIconToLocation([lat1, lon1], map, iconUrl, iconHeight, iconWidth, logUserAction);
  return (clickMarker);
}


// Function to add a transparent marker to a location on the map
var putIconToSubcidLocation = function(map, logUserAction) {
  console.log("[icon] Creating icon, location to be determined, because user "+logUserAction+".");

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

  // Define icon size // minimal size
  let iconWidth = 1
  let iconHeight = 1
  var iconUrl = 'data:image/gif;base64,R0lGODlhAQABAAD/ACwAAAAAAQABAAACADs='; // 1x1 transparent gif

  var clickMarker = _putIconToLocation(latlon, map, iconUrl, iconWidth, iconHeight, logUserAction);
  return (clickMarker);
}


// Function to add a marker to a location on the map
var _putIconToLocation = function(latlon, map, iconUrl, iconHeight, iconWidth, logUserAction) {
  console.log("[icon] Creating icon at place: "+latlon+", because user "+logUserAction+".");

  // Define icon
  let iconSize = [iconWidth, iconHeight];
  var clickIcon = L.icon({
    iconUrl: iconUrl,
    iconSize: iconSize,
    iconAnchor: [iconWidth*0.5, iconHeight], // from top left corner: go half-width to right, full-height to bottom
    popupAnchor: [0, -iconHeight-5] // where iconAnchor is, go full-height to top plus 5 pixels
  });

  // Add icon to map
  let clickMarker = L.marker(latlon, {icon: clickIcon}).addTo(map);

  // Keep icon for further use
  allMyIcons.push(clickMarker);

  return (clickMarker);
}

//////////////////////////////////
//// Generate Example Buttons ////
//////////////////////////////////

// Define programatically content/text of buttons:
var defineOneClickExampleButtons = function() {
  var examples = {
    ex1: {lon: 10.055837,         lat: 53.483139,         text: "Östlich von Hamburg", desc: "11 subcatchments"},
    ex2: {lon: 9.109039306640627, lat: 52.7810591224723,  text: "Bei Hoya", desc: "403 subcatchments"},
    ex3: {lon: 9.973955154418947, lat: 53.54193634826804, text: "Elbe in Hamburg", desc: "8071 subcatchments"},
    ex4: {lon: 9.17770385,        lat: 52.957628575,      text: "Weser bei Verden (Aller)", desc: "208433 subcatchments - only bbox works"},
    ex5: {lon: 9.921666666666667, lat: 54.69166666666666, text: "Nördlich von Kappeln (Schlei)", desc: "exactly on boundary"},
  };
  _defineExampleButtons(examples, false)
  // Finland, 47 subcatchments: 24.941250085830692, 60.385753944192324
}


// Define programatically content/text of buttons:
var defineTwoClickExampleButtons = function() {
  var examples = {
    ex1: {lon: 9.001922607421877,  lat: 52.91177308077004, lon2: 9.055137634277346,  lat2: 52.90224825087554,  text: "Near Schwarme", desc: "few segments"},
    ex2: {lon: 12.577028274536135, lat: 51.38613070285945, lon2: 12.564454078674318, lat2: 51.36524123530659,  text: "Wachtelteich", desc: "few segments"},
    ex3: {lon: 13.542366027832033, lat: 52.44028788912483, lon2: 13.283157348632814, lat2: 52.454516998017894, text: "Adlershof to Dahlem", desc: "not many segments ...via Krumme Lanke, Wannsee, Havel, Spree, Teltowkanal"},
    ex4: {lon: 12.115173339843752, lat: 53.44051847367499, lon2: 12.113800048828127, lat2: 53.149131411867074, text: "Pritzwalk to Plau via Elbe", desc: "many segments"},
    ex5: {lon: 13.051757812500002, lat: 50.85959488957681, lon2: 14.414062500000002, lat2: 51.08098143406474,  text: "Chemnitz to Wilthen via Elbe, Havel, Spree", desc: "very many segments"},
  };
  _defineExampleButtons(examples, true)
}


// Define programatically content/text of buttons:
var _defineExampleButtons = function(examples, twopairs) {

  // Define example values for buttons:
  // TODO: CODE STUPIDITY: Fill the button table and their attributes in a for loop...
  // TODO: EFFICIENCY: Have them all, just set display none if not needed!

  var buttonEx = document.getElementById("example1");
  var textEx = document.getElementById("example1desc");
  var example = examples.ex1
  buttonEx.innerHTML = example.text
  textEx.innerHTML = example.desc + " <span class=\"code\">(lon = "+example.lon+", lat = "+example.lat+")</span>";
  //textEx.innerHTML = example.desc + " <span class=\"lightsmall\">(lon = "+example.lon+", lat = "+example.lat+")</span>";
  buttonEx.setAttribute("thislon", example.lon)
  buttonEx.setAttribute("thislat", example.lat)
  if (twopairs) {
    buttonEx.setAttribute("thislon2", example.lon2)
    buttonEx.setAttribute("thislat2", example.lat2)
    buttonEx.onclick = exampleButtonClickBehaviourTwoPairs;
  } else {
    buttonEx.setAttribute("thislon2", null)
    buttonEx.setAttribute("thislat2", null)
    buttonEx.onclick = exampleButtonClickBehaviourOnePair;
  }

  var buttonEx = document.getElementById("example2");
  var textEx = document.getElementById("example2desc");
  var example = examples.ex2
  buttonEx.innerHTML = example.text
  textEx.innerHTML = example.desc + " <span class=\"code\">(lon = "+example.lon+", lat = "+example.lat+")</span>";
  //textEx.innerHTML = example.desc + " <span class=\"lightsmall\">(lon = "+example.lon+", lat = "+example.lat+")</span>";
  buttonEx.setAttribute("thislon", example.lon)
  buttonEx.setAttribute("thislat", example.lat)
  if (twopairs) {
    buttonEx.setAttribute("thislon2", example.lon2)
    buttonEx.setAttribute("thislat2", example.lat2)
    buttonEx.onclick = exampleButtonClickBehaviourTwoPairs;
  } else {
    buttonEx.setAttribute("thislon2", null)
    buttonEx.setAttribute("thislat2", null)
    buttonEx.onclick = exampleButtonClickBehaviourOnePair;
  }

  var buttonEx = document.getElementById("example3");
  var textEx = document.getElementById("example3desc");
  var example = examples.ex3
  buttonEx.innerHTML = example.text
  textEx.innerHTML = example.desc + " <span class=\"code\">(lon = "+example.lon+", lat = "+example.lat+")</span>";
  //textEx.innerHTML = example.desc + " <span class=\"lightsmall\">(lon = "+example.lon+", lat = "+example.lat+")</span>";
  buttonEx.setAttribute("thislon", example.lon)
  buttonEx.setAttribute("thislat", example.lat)
  if (twopairs) {
    buttonEx.setAttribute("thislon2", example.lon2)
    buttonEx.setAttribute("thislat2", example.lat2)
    buttonEx.onclick = exampleButtonClickBehaviourTwoPairs;
  } else {
    buttonEx.setAttribute("thislon2", null)
    buttonEx.setAttribute("thislat2", null)
    buttonEx.onclick = exampleButtonClickBehaviourOnePair;
  }

  var buttonEx = document.getElementById("example4");
  var textEx = document.getElementById("example4desc");
  var example = examples.ex4
  buttonEx.innerHTML = example.text
  textEx.innerHTML = example.desc + " <span class=\"code\">(lon = "+example.lon+", lat = "+example.lat+")</span>";
  //textEx.innerHTML = example.desc + " <span class=\"lightsmall\">(lon = "+example.lon+", lat = "+example.lat+")</span>";
  buttonEx.setAttribute("thislon", example.lon)
  buttonEx.setAttribute("thislat", example.lat)
  if (twopairs) {
    buttonEx.setAttribute("thislon2", example.lon2)
    buttonEx.setAttribute("thislat2", example.lat2)
    buttonEx.onclick = exampleButtonClickBehaviourTwoPairs;
  } else {
    buttonEx.setAttribute("thislon2", null)
    buttonEx.setAttribute("thislat2", null)
    buttonEx.onclick = exampleButtonClickBehaviourOnePair;
  }

  var buttonEx = document.getElementById("example5");
  var textEx = document.getElementById("example5desc");
  var example = examples.ex5
  buttonEx.innerHTML = example.text
  textEx.innerHTML = example.desc + " <span class=\"code\">(lon = "+example.lon+", lat = "+example.lat+")</span>";
  //textEx.innerHTML = example.desc + " <span class=\"lightsmall\">(lon = "+example.lon+", lat = "+example.lat+")</span>";
  buttonEx.setAttribute("thislon", example.lon)
  buttonEx.setAttribute("thislat", example.lat)
  if (twopairs) {
    buttonEx.setAttribute("thislon2", example.lon2)
    buttonEx.setAttribute("thislat2", example.lat2)
    buttonEx.onclick = exampleButtonClickBehaviourTwoPairs;
  } else {
    buttonEx.setAttribute("thislon2", null)
    buttonEx.setAttribute("thislat2", null)
    buttonEx.onclick = exampleButtonClickBehaviourOnePair;
  }
}


////////////////////////////////////
//// Example Buttons: Behaviour ////
////////////////////////////////////

// Define behaviour for example button (function):
var exampleButtonClickBehaviourOnePair = function() {
  console.log("Button click: User requested use of example values...")

  // Which coordinates
  var caller = event.target;
  var lon1 = caller.getAttribute("thislon");
  var lat1 = caller.getAttribute("thislat");
  console.log("Button click: One coordinate pair: "+lon1+", "+lat1+" (lon, lat, WGS84)");

  // Which process?
  var dropdown = document.getElementById("processes");
  var processId = dropdown.value;
  var processDesc = dropdown.options[dropdown.selectedIndex].text;
  console.log('Button click: When clicked, this process was selected: '+processId+' ('+processDesc+').');

  // Add icon and popup to click location, scroll map up:
  clickMarker = putIconToClickLocation(lon1, lat1, map, "clicked on button");
  document.getElementById("scrollToTop").scrollIntoView();

  // Construct and send HTTP request to OGC service:
  ogcRequestOneCoordinatePair(clickMarker, processId, lon1, lat1, processDesc);
}

// Define behaviour for example button (function):
var exampleButtonClickBehaviourTwoPairs = function() {
  console.log("Button click: User requested use of example values...")

  // Which coordinates
  var caller = event.target;
  var lon1 = caller.getAttribute("thislon");
  var lat1 = caller.getAttribute("thislat");
  var lon2 = caller.getAttribute("thislon2");
  var lat2 = caller.getAttribute("thislat2");
  console.log("Button click: Two coordinate pairs: "+lon1+", "+lat1+" and "+lon2+", "+lat2+" (lon, lat, WGS84)")

  // Which process?
  var dropdown = document.getElementById("processes");
  var processId = dropdown.value;
  var processDesc = dropdown.options[dropdown.selectedIndex].text;
  console.log('Button click: When clicked, this process was selected: '+processId+' ('+processDesc+').');

  // Add icon and popup to click location, scroll map up:
  clickMarker = putIconToClickLocation(lon1, lat1, map, "clicked on button (part 1)");
  clickMarker = putIconToClickLocation(lon2, lat2, map, "clicked on button (part 2)");
  document.getElementById("scrollToTop").scrollIntoView();

  // Construct and send HTTP request to OGC service:
  ogcRequestTwoCoordinatePairs(clickMarker, processId, lon1, lat1, lon2, lat2, processDesc)
}





// If lonlat is changed by user, also change lon and lat:
var eventLonLat1Changed = function(evt) {
  var temp = this.value.split(",");
  var newlon = parseFloat(temp[0]);
  var newlat = parseFloat(temp[1]);
  document.getElementById("customLon1").value = newlon;
  document.getElementById("customLat1").value = newlat;
  document.getElementById("customSubc1").value = "";
};

// If lon is changed by the user, also change lonlat
var eventLon1Changed = function(evt) {
  var newlon = parseFloat(this.value);
  var oldlonlat = document.getElementById("customLonLat1").value;
  if (oldlonlat === "") { oldlonlat = "undefined,undefined" };
  var oldlat = oldlonlat.split(",")[1]
  var newlonlat = newlon+', '+oldlat;
  // This adds more and more spaces, so we remove double whitespace:
  newlonlat = newlonlat.replace("  ", " ");
  document.getElementById("customLonLat1").value = newlonlat;
  document.getElementById("customSubc1").value = "";
};

// If lat is changed by the user, also change lonlat
var eventLatChanged = function(evt) {
  var newlat = parseFloat(this.value);
  var oldlonlat = document.getElementById("customLonLat1").value;
  if (oldlonlat === "") { oldlonlat = "undefined,undefined" };
  var oldlon = oldlonlat.split(",")[0]
  var newlonlat = oldlon+', '+newlat;
  document.getElementById("customLonLat1").value = newlonlat;
  document.getElementById("customSubc1").value = "";
};

// If the user adds a subc_id, remove all lon/lat coords:
var eventSubcid1Changed = function(evt) {
  if (this.value == "") {
    console.log("FIRED: eventSubcid1Changed, with no value.");
  } else {
    console.log("FIRED: eventSubcid1Changed, with value "+this.value+" (now parsing to int).");
    var newsubcid = parseInt(this.value);
    document.getElementById("customSubc1").value = newsubcid;
    // No more coordinates:
    document.getElementById("customLon1").value = "";
    document.getElementById("customLat1").value = "";
    document.getElementById("customLonLat1").value = "";
  }
};

// When a user focuses on a coordinate field, empty the subcatchment field!
var eventCoord1Focus = function(evt) {
  document.getElementById("customSubc1").value = "";
};
var eventCoord2Focus = function(evt) {
  document.getElementById("customSubc2").value = "";
};

// Same for the second set, which is usually invisible, unless
// when a process is selected that needs two locations (routing)
var eventLonLat2Changed = function(evt) {
  var temp = this.value.split(",");
  var newlon = parseFloat(temp[0]);
  var newlat = parseFloat(temp[1]);
  document.getElementById("customLon2").value = newlon;
  document.getElementById("customLat2").value = newlat;
  document.getElementById("customSubc2").value = "";
};

var eventLon2Changed = function(evt) {
  var newlon = parseFloat(this.value);
  var oldlonlat = document.getElementById("customLonLat2").value;
  if (oldlonlat === "") { oldlonlat = "undefined,undefined" };
  var newlonlat = newlon+', '+oldlonlat.split(",")[1];
  document.getElementById("customLonLat2").value = newlonlat;
  document.getElementById("customSubc2").value = "";
};

// Define event listeners for custom lon and lat
var eventLat2Changed = function(evt) {
  var newlat = parseFloat(this.value);
  var oldlonlat = document.getElementById("customLonLat2").value;
  if (oldlonlat === "") { oldlonlat = "undefined,undefined" };
  var newlonlat = oldlonlat.split(",")[0]+', '+newlat;
  document.getElementById("customLonLat2").value = newlonlat;
  document.getElementById("customSubc2").value = "";
};

var eventSubcid2Changed = function(evt) {
  if (this.value == "") {
    console.log("FIRED: eventSubcid2Changed, with no value.");
  } else {
    console.log("FIRED: eventSubcid2Changed, with value "+this.value+" (now parsing to int).");
    var newsubcid = parseInt(this.value);
    document.getElementById("customSubc2").value = newsubcid;
    // No more coordinates:
    document.getElementById("customLon2").value = "";
    document.getElementById("customLat2").value = "";
    document.getElementById("customLonLat2").value = "";
  }
};
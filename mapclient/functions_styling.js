

///////////////////////////////////////////////
////// Functions for colouring / styling //////
///////////////////////////////////////////////

// Colour conversion function for strahler-colouring.
// TODO: Move to separate javascript and import
var convert_hsl_to_hex = function(h, s, l) {

  let hsl2rgb = function(h,s,l) {
    // Source:
    // https://stackoverflow.com/questions/2353211/hsl-to-rgb-color-conversion
    let a=s*Math.min(l,1-l);
    let f= (n,k=(n+h/30)%12) => l - a*Math.max(Math.min(k-3,9-k,1),-1);
    //return [f(0),f(8),f(4)]; // values between 0 and 1
    return [f(0)*255,f(8)*255,f(4)*255];
  }

  let componentToHex = function(c) {
    // Source:
    // https://stackoverflow.com/questions/5623838/rgb-to-hex-and-hex-to-rgb#5624139
    //console.log(' (rgb val '+c+')')
    let rounded = Math.round(c)
    //console.log(' (rounded '+rounded+')')
    var hex = rounded.toString(16);
    //console.log(' (hexed '+hex+')')
    return hex.length == 1 ? "0" + hex : hex;
  }

  let rgbToHex = function(r, g, b) {
    return "#" + componentToHex(r) + componentToHex(g) + componentToHex(b);
  }

  let col_rgb = hsl2rgb(h, s, l);
  let col_hex = rgbToHex(col_rgb[0], col_rgb[1], col_rgb[2]);
  return (col_hex);
}

// Lightness conversion function for strahler-colouring.
// TODO: Move to separate javascript and import
// TODO: EFFICIENCY: Would be good to precompute, as we have few strahler-orders, and many features...
var strahler_to_hsl_lightness = function(strahler, lowest_lightness, highest_lightness) {
  strahler = parseInt(strahler);
  let tmp = 11-strahler;

  // Make zero if negative, for strahler > 11, i.e. rivers bigger than Danube!
  if (tmp < 0) {
    tmp = 0;
  }

  //let lightness = 0.2 + ((0.9-0.2)*0.1) * tmp
  let lightness = lowest_lightness + (highest_lightness - lowest_lightness) * tmp * 0.1
  return lightness
}

// Line weight conversion function for strahler-colouring.
// TODO: Move to separate javascript and import
// TODO: EFFICIENCY: Would be good to precompute, as we have few strahler-orders, and many features...
var strahler_to_line_weight = function(strahler, min_weight=2, max_weight=4) {
  let slope = (max_weight - min_weight)/10 // 10 is approximately strahler spread...
  let add = min_weight - slope * 1 // 1 is minimum strahler
  let weight = add + slope*strahler
  //console.log('Strahler: '+strahler+', line weight: '+weight);
  return(weight)
}

var styleLayerUni = function(layer) {
  // Depending on process:
  // Upstream bbox = Grey
  // Upstream (segments, catchments) = Navy
  // Local (segment, catchment) = Blue #3366cc
  // Downstream = Light blue
  // Connecting line = red dashed
  // TODO: In upstream catchment, distinguish local and upstream (in feature properties, then here style!)
  // TODO: Display number of subcatchment? Does server return them?

  // Which process?
  var dropdown = document.getElementById("processes");
  var processId = dropdown.value;

  if (processId == "get-upstream-bbox") {
    layer.setStyle({fillColor: 'grey', color: 'grey'});

  } else if (processId == "get-shortest-path-to-outlet") {
    layer.setStyle({color: '#00b8e6', weight: 8});
    // This layer can already be styled based on strahler!

  } else if (processId == "get-upstream-dissolved-cont") {
    layer.setStyle({color: 'navy', weight: 5});

  } else if (processId == "get-upstream-subcatchments") {
    layer.setStyle({color: 'navy', weight: 1});

  } else if (processId == "get-upstream-streamsegments") {
    layer.setStyle({color: 'navy', weight: 4});
    // This layer can already be styled based on strahler!

  } else if (processId == "get-local-streamsegments") {
    layer.setStyle({color: '#3366cc', weight: 4});

  } else if (processId == "get-local-streamsegments-subcatchments") {
    if (layer.feature.geometry.type == 'LineString') {
      layer.setStyle({fillColor: '#3366cc', color: '#3366cc', weight: 4});
    } else if (layer.feature.geometry.type == 'MultiPolygon') {
      layer.setStyle({color: '#3366cc', fillColor: '#3366cc', weight: 0});
    } else {
      console.log('TODO: Found feature that is neither LineString nor MultiPolygon. You may want to check this.');
      console.log('PAINTED IT PINK!')
      layer.setStyle({fillColor: 'pink', color: 'pink', weight: 3});
    };

  } else if (processId == "get-shortest-path-between-points") {
    layer.setStyle({color: '#00b8e6', weight: 8});
    // This layer can already be styled based on strahler!

  } else if (processId == "get-snapped-point-plus") {
    if (layer.feature.properties.description == 'connecting line') {
      layer.setStyle({fillColor: 'red', color: 'red', weight: 1, dashArray: '8 4'});
      console.log('Found a connecting line and painted it dashed red...');
    } else if (layer.feature.geometry.type == 'LineString') {
      layer.setStyle({fillColor: '#3366cc', color: '#3366cc', weight: 4});
    } else if (layer.feature.geometry.type == 'MultiPolygon') {
      layer.setStyle({fillColor: '#3366cc', color: '#3366cc', weight: 0});
    } else if (layer.feature.geometry.type == 'Point') {
      // TODO How to style points?
    } else {
      console.log('TODO: Found feature that is neither LineString nor MultiPolygon nor Point. You may want to check this.');
      console.log('PAINTED IT PINK!')
      layer.setStyle({fillColor: 'pink', color: 'pink', weight: 3});
    };

  } else if (processId == "get-snapped-points") {
      // TODO How to style points?
      //layer.setStyle({color: 'black', weight: 3});
      //console.log('Found snapped point and painted them black...');

  // All others:
  } else {
    console.log('TODO: Found feature that is not styled yet. You may want to check this.');
    console.log('PAINTED IT PINK!')
    layer.setStyle({fillColor: 'pink', color: 'pink', weight: 3});
  }
};

var styleLayerStrahler = function(layer, processId) {
  // Depending on process:
  // Upstream bbox = Grey
  // Upstream (segments, catchments) = Navy
  // Local (segment, catchment) = Blue #3366cc
  // Downstream = Light blue
  // Connecting line = red dashed
  // TODO: In upstream catchment, distinguish local and upstream (in feature properties, then here style!)
  // TODO: Display number of subcatchment? Does server return them?

  // Which process?
  //console.log("[DEBUG] processId: "+processId);

  if (processId == "get-upstream-bbox") {
    console.log("[DEBUG] Styling based on strahler order makes no sense: "+processId);
    layer.setStyle({fillColor: 'grey', color: 'grey'});
    // Cannot differentiate by strahler: Just a bbox!

  } else if (processId == "get-shortest-path-to-outlet") {
    console.log("[DEBUG] Styling based on strahler order makes sense: "+processId);
    // let's vary colour with strahler order, more precisely with
    // lightness in the HSL colour model:
    // Blue: hue=192, saturation=1
    //  lightness = 0.5 or 50% medium blue
    //  lightness = 0.2 or 20% black-ish
    //  lightness = 0.9 or 90% quite light
    // Reasonable base colour to start with. Now vary lightness with strahler...
    let hue = 192;
    let saturation = 1;
    let lightness = 0.5

    // Get from strahler to a lightness value between 0.2 and 0.9!
    let strahler = layer.feature.properties.strahler_order;
    let lowest_lightness = 0.2
    let highest_lightness = 0.9
    lightness = strahler_to_hsl_lightness(strahler, lowest_lightness, highest_lightness);
    let col_hex = convert_hsl_to_hex(hue, 1, lightness);
    layer.setStyle({color: col_hex, weight: 8});

  } else if (processId == "get-upstream-dissolved-cont") {
    console.log("[DEBUG] Styling based on strahler order makes no sense: "+processId);
    layer.setStyle({color: 'navy', weight: 5});
    // Cannot differentiate by strahler: Polygons are dissolved!

  } else if (processId == "get-upstream-subcatchments") {
    console.log("[DEBUG] Styling based on strahler order makes sense BUT is not implemented yet: "+processId);
    layer.setStyle({color: 'navy', weight: 1});
    // TODO! Strahler-Style for upstream catchments

  } else if (processId == "get-upstream-streamsegments") {
    console.log("[DEBUG] Styling based on strahler order makes sense: "+processId);
    // let's vary colour with strahler order, more precisely with
    // lightness in the HSL colour model:
    // Navy: hue=240, saturation=1
    //  lightness = 0.25 or 25% medium blue
    //  lightness = 0.2 or 20% black-ish
    //  lightness = 0.9 or 90% quite light
    // Reasonable base colour to start with. Now vary lightness with strahler...
    let hue = 240;
    let saturation = 1;
    let lightness = 0.25

    // Get from strahler to a lightness value between 0.2 and 0.9!
    let strahler = layer.feature.properties.strahler_order;
    let lowest_lightness = 0
    let highest_lightness = 0.8
    lightness = strahler_to_hsl_lightness(strahler, lowest_lightness, highest_lightness);
    let col_hex = convert_hsl_to_hex(hue, 1, lightness);

    // Get from strahler to a line weight value between 2 and 4!
    let min_weight = 2;
    let max_weight = 4;
    let weight = strahler_to_line_weight(strahler, min_weight, max_weight)
    layer.setStyle({color: col_hex, weight: weight});

  } else if (processId == "get-local-streamsegments") {
    // TODO MAYBE! Strahler-Style, although it is not great for local stuff...
    console.log("[DEBUG] Styling based on strahler order makes partially sense, BUT is not implemented yet: "+processId);
    layer.setStyle({color: '#3366cc', weight: 4});

  } else if (processId == "get-local-streamsegments-subcatchments") {
    /// TODO MAYBE! Strahler-Style, although it is not great for local stuff...
    console.log("[DEBUG] Styling based on strahler order makes partially sense, BUT is not really implemented yet: "+processId);
    if (layer.feature.geometry.type == 'LineString') {
      layer.setStyle({fillColor: '#3366cc', color: '#3366cc', weight: 4});
    } else if (layer.feature.geometry.type == 'MultiPolygon') {
      layer.setStyle({color: '#3366cc', fillColor: '#3366cc', weight: 0});
    } else {
      console.log('TODO: Found feature that is neither LineString nor MultiPolygon. You may want to check this.');
      console.log('PAINTED IT PINK!')
      layer.setStyle({fillColor: 'pink', color: 'pink', weight: 3});
    };

  } else if (processId == "get-shortest-path-between-points") {
    console.log("[DEBUG] Styling based on strahler order makes sense: "+processId);
    // let's vary colour with strahler order, more precisely with
    // lightness in the HSL colour model:
    // Blue: hue=192, saturation=1
    //  lightness = 0.5 or 50% medium blue
    //  lightness = 0.2 or 20% black-ish
    //  lightness = 0.9 or 90% quite light
    // Reasonable base colour to start with. Now vary lightness with strahler...
    let hue = 192;
    let saturation = 1;
    let lightness = 0.5

    // Get from strahler to a lightness value between 0.2 and 0.9!
    let strahler = layer.feature.properties.strahler_order;
    let lowest_lightness = 0.2
    let highest_lightness = 0.9
    lightness = strahler_to_hsl_lightness(strahler, lowest_lightness, highest_lightness);
    //console.log('Looking for colour of lightness '+lightness);
    let col_hex = convert_hsl_to_hex(hue, 1, lightness);
    //console.log('Colour in HEX: '+col_hex);
    layer.setStyle({color: col_hex, weight: 8});

  } else if (processId == "get-snapped-point-plus") {
    // TODO MAYBE! Strahler-Style, although it is not great for local stuff...
    console.log("[DEBUG] Styling based on strahler order makes partially sense, BUT is not really implemented yet: "+processId);
    if (layer.feature.properties.description == 'connecting line') {
      layer.setStyle({fillColor: 'red', color: 'red', weight: 1, dashArray: '8 4'});
      console.log('Found a connecting line and painted it dashed red...');
    } else if (layer.feature.geometry.type == 'LineString') {
      layer.setStyle({fillColor: '#3366cc', color: '#3366cc', weight: 4});
    } else if (layer.feature.geometry.type == 'MultiPolygon') {
      layer.setStyle({fillColor: '#3366cc', color: '#3366cc', weight: 0});
    } else if (layer.feature.geometry.type == 'Point') {
      // TODO How to style points?
    } else {
      console.log('TODO: Found feature that is neither LineString nor MultiPolygon nor Point. You may want to check this.');
      console.log('PAINTED IT PINK!')
      layer.setStyle({fillColor: 'pink', color: 'pink', weight: 3});
    };

  } else if (processId == "get-snapped-points") {
    console.log("[DEBUG] Styling based on strahler, how to do this: "+processId);
      // TODO How to style points?
      //layer.setStyle({color: 'black', weight: 3});
      //console.log('Found snapped point and painted them black...');

  // All others:
  } else {
    console.log("[DEBUG] No styling info for process: "+processId);
    console.log('TODO: Found feature that is not styled yet. You may want to check this.');
    console.log('PAINTED IT PINK!')
    layer.setStyle({fillColor: 'pink', color: 'pink', weight: 3});
  }
};

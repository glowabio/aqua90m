function downloadGeoJson() {
    const text = document.getElementById("displayGeoJSON").value;

    const blob = new Blob([text], { type: "text/plain" });
    const url = URL.createObjectURL(blob);

    const a = document.createElement("a");
    a.href = url;
    a.download = "igb_geojson_result.json";
    a.click();

    URL.revokeObjectURL(url);
};

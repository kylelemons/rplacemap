const map = L.map('map').setView([0,0], 0);

L.tileLayer('/tiles/{x}_{y}_z{z}_{tileSize}x{tileSize}.png', {
    maxZoom: 10,
    tileSize: 256,
    zoomOffset: 0,
    // bounds: L.latLngBounds(
    //   L.latLng(-1001, -1001),
    //   L.latLng(1001, 1001),
    // ),
    //noWrap: true,
}).addTo(map);

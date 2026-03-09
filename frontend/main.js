// Basic configuration
const BACKEND_BASE_URL = "http://127.0.0.1:8000";

const state = {
  selectedRoute: null,
  patternId: null,
  directionId: "",
  dateYMD: null,
  prettyOsm: false,
  stops: [],
  startStopId: null,
  endStopId: null,
  // Single polygon/line geometry used both as "area of interest" for
  // /area/routes queries and as blockage geometry for /detour.
  areaGeoJSON: null,
  map: null,
  layers: {
    routeEdges: null,
    routeStops: null,
    snappedPattern: null,
    blockage: null,
    detour: null,
  },
  areaRoutes: [],
  /** Coords for each route polyline [ [lat,lon], ... ], used to redraw direction triangles on zoom. */
  _routeLineCoords: [],
};

function showToast(message, { error = false, timeout = 3000 } = {}) {
  const el = document.getElementById("toast");
  if (!el) return;
  el.textContent = message;
  el.classList.toggle("error", !!error);
  el.classList.remove("hidden");
  if (timeout) {
    setTimeout(() => {
      el.classList.add("hidden");
    }, timeout);
  }
}

async function api(path, options = {}) {
  const url = `${BACKEND_BASE_URL}${path}`;
  const resp = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!resp.ok) {
    let detail = resp.statusText;
    try {
      const data = await resp.json();
      detail = data.detail || JSON.stringify(data);
    } catch {
      // ignore
    }
    throw new Error(`API ${resp.status}: ${detail}`);
  }
  if (resp.status === 204) return null;
  return resp.json();
}

function initMap() {
  const map = L.map("map", {
    center: [31.8, 35.0], // roughly center of Israel
    zoom: 8,
  });

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  }).addTo(map);

  state.map = map;

  // Panes for stable z-order: route under detour under stops under arrows.
  map.createPane("routePane");
  map.getPane("routePane").style.zIndex = 400;
  map.createPane("detourPane");
  map.getPane("detourPane").style.zIndex = 450;
  map.createPane("stopsPane");
  map.getPane("stopsPane").style.zIndex = 650;
  map.createPane("arrowsPane");
  map.getPane("arrowsPane").style.zIndex = 660;

  // Feature groups for overlays (order: stops-in-view, route line, then direction triangles on top of line, then stops)
  state.layers.stopsInView = L.featureGroup().addTo(map);
  state.layers.routeEdges = L.featureGroup().addTo(map);
  state.layers.directionTriangles = L.featureGroup().addTo(map); // on top of route line so triangles are never hidden
  state.layers.routeStops = L.featureGroup().addTo(map);
  state.layers.snappedPattern = L.featureGroup().addTo(map);
  state.layers.blockage = L.featureGroup().addTo(map);
  state.layers.detour = L.featureGroup().addTo(map);

  // Load clickable stops in view when zoomed in (zoom >= 14)
  let stopsLoadTimeout = null;
  function scheduleLoadStopsInView() {
    if (stopsLoadTimeout) clearTimeout(stopsLoadTimeout);
    stopsLoadTimeout = setTimeout(loadStopsInView, 200);
  }
  map.on("moveend", () => {
    scheduleLoadStopsInView();
    redrawDirectionTriangles();
  });
  map.on("zoomend", () => {
    scheduleLoadStopsInView();
    redrawDirectionTriangles();
  });
  scheduleLoadStopsInView();

  // Draw controls for area polygon / blockage
  const drawControl = new L.Control.Draw({
    draw: {
      polygon: true,
      polyline: true,
      rectangle: true,
      circle: false,
      marker: false,
      circlemarker: false,
    },
    edit: {
      featureGroup: state.layers.blockage,
      edit: false,
      remove: false,
    },
  });
  map.addControl(drawControl);

  map.on(L.Draw.Event.CREATED, (e) => {
    // Clear previous polygon / blockage
    state.layers.blockage.clearLayers();
    state.layers.detour.clearLayers();
    const layer = e.layer;
    state.layers.blockage.addLayer(layer);
    // Store geometry only (GeoJSON spec); backend expects Geometry, not Feature.
    state.areaGeoJSON = layer.toGeoJSON().geometry;
    const canCompute = !!state.areaGeoJSON;
    document.getElementById("btn-compute-detour").disabled = !canCompute;

    if (canCompute) {
      showToast("Blockage drawn. Click «Compute detour» (route-only or all lines in area).", {
        timeout: 3500,
      });
    }
  });

  // Ensure map fills the flex container once layout is applied
  requestAnimationFrame(() => {
    map.invalidateSize();
  });
  window.addEventListener("resize", () => {
    map.invalidateSize();
  });
}

const MIN_ZOOM_FOR_STOPS = 14;

async function loadStopsInView() {
  const map = state.map;
  if (!map || map.getZoom() < MIN_ZOOM_FOR_STOPS) {
    state.layers.stopsInView.clearLayers();
    return;
  }
  const bounds = map.getBounds();
  const min_lat = bounds.getSouthWest().lat;
  const max_lat = bounds.getNorthEast().lat;
  const min_lon = bounds.getSouthWest().lng;
  const max_lon = bounds.getNorthEast().lng;
  const params = new URLSearchParams({
    min_lat: min_lat.toFixed(6),
    max_lat: max_lat.toFixed(6),
    min_lon: min_lon.toFixed(6),
    max_lon: max_lon.toFixed(6),
    limit: "500",
  });
  try {
    const stops = await api(`/stops/in-bounds?${params}`);
    state.layers.stopsInView.clearLayers();
    stops.forEach((stop) => {
      const marker = L.circleMarker([stop.stop_lat, stop.stop_lon], {
        radius: 6,
        color: "#6366f1",
        weight: 1,
        fillColor: "#a5b4fc",
        fillOpacity: 0.9,
      }).addTo(state.layers.stopsInView);
      const popupEl = document.createElement("div");
      popupEl.className = "stop-info-popup";
      popupEl.innerHTML = buildStopPopupContent(stop, null);
      marker.bindPopup(popupEl, { maxWidth: 320 });
      marker.on("popupopen", () => {
        bindStopPopupFindLines(popupEl, stop, marker);
      });
    });
    // Ensure route stops/edges stay visually above the generic stops layer.
    if (state.layers.routeStops) state.layers.routeStops.bringToFront();
    if (state.layers.routeEdges) state.layers.routeEdges.bringToFront();
  } catch (err) {
    console.warn("Failed to load stops in view:", err);
  }
}

function buildStopPopupContent(stop, routesResult) {
  const name = stop.stop_name || stop.stop_id;
  const code = stop.stop_code ? ` · #${stop.stop_code}` : "";
  let linesSection = "";
  if (routesResult === null) {
    linesSection = `
      <div class="stop-popup-lines">
        <div class="stop-popup-lines-title">Lines at this stop</div>
        <div class="field-row">
          <input type="time" id="popup-time-start" value="07:00" />
          <span class="hint">to</span>
          <input type="time" id="popup-time-end" value="10:00" />
        </div>
        <button type="button" id="popup-find-lines" class="find-lines-btn">Find lines</button>
        <div id="popup-lines-result" class="popup-lines-result"></div>
      </div>
    `;
  } else if (routesResult.routes && routesResult.routes.length > 0) {
    const list = routesResult.routes
      .slice(0, 25)
      .map(
        (r) =>
          `<div class="popup-route-item"><strong>${r.route_short_name || r.route_id}</strong>${r.direction_id !== null && r.direction_id !== undefined && r.direction_id !== "" ? ` (dir ${r.direction_id})` : ""} — ${r.route_long_name || ""} <span class="hint">${r.first_time || ""}–${r.last_time || ""}</span></div>`
      )
      .join("");
    const more = routesResult.routes.length > 25 ? `<div class="hint">+ ${routesResult.routes.length - 25} more</div>` : "";
    linesSection = `
      <div class="stop-popup-lines">
        <div class="stop-popup-lines-title">Lines (${routesResult.routes.length})</div>
        <div class="popup-lines-list">${list}${more}</div>
      </div>
    `;
  } else {
    linesSection = `
      <div class="stop-popup-lines">
        <div class="stop-popup-lines-title">Lines at this stop</div>
        <div class="popup-lines-result"><em>No lines in this time window.</em></div>
      </div>
    `;
  }
  return `
    <div class="stop-popup-title">${name}</div>
    <div class="stop-popup-meta">Stop ID: ${stop.stop_id}${code}</div>
    ${linesSection}
  `;
}

function bindStopPopupFindLines(popupEl, stop, marker) {
  const findBtn = popupEl.querySelector("#popup-find-lines");
  if (!findBtn) return;
  const timeStart = popupEl.querySelector("#popup-time-start");
  const timeEnd = popupEl.querySelector("#popup-time-end");
  const resultEl = popupEl.querySelector("#popup-lines-result");
  const dateInput = document.getElementById("route-date");
  const ymd = dateInput ? ymdFromDateInput(dateInput.value) : null;
  if (!ymd) {
    if (resultEl) resultEl.innerHTML = "<em>Pick a date in the sidebar first.</em>";
    return;
  }
  findBtn.onclick = async () => {
    if (resultEl) resultEl.innerHTML = "<em>Searching…</em>";
    try {
      const res = await api("/stop/routes", {
        method: "POST",
        body: JSON.stringify({
          stop_id: stop.stop_id,
          date: ymd,
          start_time: timeStart ? timeStart.value || "00:00" : "00:00",
          end_time: timeEnd ? timeEnd.value || "23:59" : "23:59",
          max_results: 100,
        }),
      });
      const newContent = buildStopPopupContent(stop, res);
      popupEl.innerHTML = newContent;
      marker.setPopupContent(popupEl);
      bindStopPopupFindLines(popupEl, stop, marker);
    } catch (err) {
      if (resultEl) resultEl.innerHTML = `<em>Error: ${err.message}</em>`;
    }
  };
}

function ymdFromDateInput(value) {
  if (!value) return null;
  return value.replace(/-/g, "");
}

function updateSelectedStopsSummary() {
  const el = document.getElementById("selected-stops");
  if (!el) return;
  if (!state.startStopId && !state.endStopId) {
    el.innerHTML = "<em>No stops selected yet.</em>";
    return;
  }
  const start = state.stops.find((s) => s.stop_id === state.startStopId);
  const end = state.stops.find((s) => s.stop_id === state.endStopId);
  el.innerHTML = `
    <div><strong>Start:</strong> ${
      start ? `${start.name} (${start.stop_id})` : "—"
    }</div>
    <div><strong>End:</strong> ${
      end ? `${end.name} (${end.stop_id})` : "—"
    }</div>
  `;
}

function updateStopsSummaryBox() {
  const el = document.getElementById("stops-summary");
  if (!el) return;
  if (!state.stops || state.stops.length === 0) {
    el.innerHTML = "<em>No stops loaded. Build a graph first.</em>";
    return;
  }
  const first = state.stops[0];
  const last = state.stops[state.stops.length - 1];
  el.innerHTML = `
    <div class="summary-row">
      <span><strong>Stops</strong>: ${state.stops.length}</span>
      <span><strong>Pattern</strong>: ${state.patternId || "?"}</span>
    </div>
    <div style="margin-top:4px;">
      <div><strong>From</strong>: ${first.name} (${first.stop_id})</div>
      <div><strong>To</strong>: ${last.name} (${last.stop_id})</div>
    </div>
  `;
}

/**
 * Add direction arrowheads along a polyline (L.polygon).
 * Clamped size and zoom-aware spacing: fewer, smaller arrows when zoomed out.
 * Narrower arrowhead shape for clearer direction.
 */
function addDirectionTrianglesAlongLine(coords, layer, options) {
  if (!coords || coords.length < 2) return;

  const opts = options || {};
  const map = opts.map || state.map;
  const zoom = map ? map.getZoom() : 14;

  // Bigger arrows at low zoom so they stay visible.
  const sizeByZoom =
    zoom <= 8 ? 32 :
    zoom <= 9 ? 26 :
    zoom <= 10 ? 21 :
    zoom <= 11 ? 17 :
    zoom <= 12 ? 14 :
    zoom <= 14 ? 11 : 9;

  // Much wider spacing when zoomed out so the line isn't flooded.
  const spacingByZoom =
    zoom <= 8 ? 1200 :
    zoom <= 9 ? 900 :
    zoom <= 10 ? 650 :
    zoom <= 11 ? 420 :
    zoom <= 12 ? 260 :
    zoom <= 14 ? 150 : 100;

  const size = opts.sizeMeters ?? sizeByZoom;
  const spacingMeters = opts.spacingMeters ?? spacingByZoom;
  const color = opts.color || "#111827";
  const fillColor = opts.fillColor || "#ffffff";

  const toRad = (deg) => (deg * Math.PI) / 180;
  const toDeg = (rad) => (rad * 180) / Math.PI;

  const haversineM = (lat1, lon1, lat2, lon2) => {
    const R = 6371000;
    const dlat = toRad(lat2 - lat1);
    const dlon = toRad(lon2 - lon1);
    const a =
      Math.sin(dlat / 2) ** 2 +
      Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dlon / 2) ** 2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  };

  const offsetPoint = (lat, lon, bearingDeg, distM) => {
    const R = 6371000;
    const d = distM / R;
    const br = toRad(bearingDeg);
    const latR = toRad(lat);
    const lat2 = Math.asin(
      Math.sin(latR) * Math.cos(d) +
      Math.cos(latR) * Math.sin(d) * Math.cos(br)
    );
    const lon2 = toDeg(
      toRad(lon) +
      Math.atan2(
        Math.sin(br) * Math.sin(d) * Math.cos(latR),
        Math.cos(d) - Math.sin(latR) * Math.sin(lat2)
      )
    );
    return [toDeg(lat2), lon2];
  };

  const bearing = (lat1, lon1, lat2, lon2) => {
    const dlon = toRad(lon2 - lon1);
    const y = Math.sin(dlon) * Math.cos(toRad(lat2));
    const x =
      Math.cos(toRad(lat1)) * Math.sin(toRad(lat2)) -
      Math.sin(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.cos(dlon);
    return (toDeg(Math.atan2(y, x)) + 360) % 360;
  };

  const cumul = [0];
  for (let i = 1; i < coords.length; i++) {
    cumul[i] =
      cumul[i - 1] +
      haversineM(
        coords[i - 1][0], coords[i - 1][1],
        coords[i][0], coords[i][1]
      );
  }

  const total = cumul[cumul.length - 1];
  if (!total || total < spacingMeters) return;

  const samples = [];
  for (let d = spacingMeters * 0.5; d < total; d += spacingMeters) {
    let i = 0;
    while (i < cumul.length - 1 && cumul[i + 1] < d) i++;
    if (i >= cumul.length - 1) break;

    const segLen = cumul[i + 1] - cumul[i];
    if (!segLen) continue;

    const t = (d - cumul[i]) / segLen;
    const lat = coords[i][0] + t * (coords[i + 1][0] - coords[i][0]);
    const lon = coords[i][1] + t * (coords[i + 1][1] - coords[i][1]);
    const b = bearing(coords[i][0], coords[i][1], coords[i + 1][0], coords[i + 1][1]);
    samples.push({ lat, lon, b });
  }

  // Longer/narrower arrowhead so direction stays readable.
  for (const { lat, lon, b } of samples) {
    const tip = offsetPoint(lat, lon, b, size);
    const rear = offsetPoint(lat, lon, b + 180, size * 0.7);
    const base1 = offsetPoint(rear[0], rear[1], b + 90, size * 0.28);
    const base2 = offsetPoint(rear[0], rear[1], b - 90, size * 0.28);

    L.polygon([tip, base1, base2], {
      color: "#111827",
      fillColor: "#ffffff",
      fillOpacity: 0.98,
      weight: 1.6,
      interactive: false,
      pane: "arrowsPane",
    }).addTo(layer);
  }
}

const DIRECTION_TRIANGLE_OPTS = {};

function redrawDirectionTriangles() {
  if (!state.map || !state.layers.directionTriangles) return;
  state.layers.directionTriangles.clearLayers();
  const lines = state._routeLineCoords || [];
  const opts = { ...DIRECTION_TRIANGLE_OPTS, map: state.map };
  lines.forEach((coords) => {
    addDirectionTrianglesAlongLine(coords, state.layers.directionTriangles, opts);
  });
}

function renderRouteGeoJSON(geojson) {
  const map = state.map;
  if (!map) return;

  state.layers.routeEdges.clearLayers();
  state.layers.directionTriangles.clearLayers();
  state.layers.routeStops.clearLayers();
  state.layers.snappedPattern.clearLayers();
  state.layers.detour.clearLayers();
  state._routeLineCoords = [];

  const features = geojson.features || [];

  const edgeStyle = {
    color: "#16a34a",
    weight: 4,
    opacity: 0.9,
    pane: "routePane",
  };

  const snappedStyle = {
    color: "#15803d",
    weight: 5,
    opacity: 0.9,
    pane: "routePane",
  };

  const stopIcon = L.circleMarker;

  const edgeLayers = [];

  // Precompute sequences to distinguish first/last stop on the route.
  const seqByStopId = {};
  let minSeq = null;
  let maxSeq = null;
  if (Array.isArray(state.stops)) {
    state.stops.forEach((s) => {
      if (s && typeof s.sequence === "number") {
        seqByStopId[s.stop_id] = s.sequence;
        if (minSeq === null || s.sequence < minSeq) minSeq = s.sequence;
        if (maxSeq === null || s.sequence > maxSeq) maxSeq = s.sequence;
      }
    });
  }

  features.forEach((f) => {
    const geomType = f.geometry && f.geometry.type;
    const props = f.properties || {};

    if (geomType === "Point" && props.stop_id) {
      const [lon, lat] = f.geometry.coordinates;
      const seq =
        seqByStopId[props.stop_id] !== undefined ? seqByStopId[props.stop_id] : null;

      // Default style for route stops (distinct from "all stops in view).
      let markerOptions = {
        radius: 5,
        color: "#f97316",
        weight: 1,
        fillColor: "#fed7aa",
        fillOpacity: 0.9,
        pane: "stopsPane",
      };

      // Highlight first and last stop with different colors / sizes.
      if (seq !== null && minSeq !== null && seq === minSeq) {
        // First stop: green
        markerOptions = {
          radius: 7,
          color: "#22c55e",
          weight: 2,
          fillColor: "#bbf7d0",
          fillOpacity: 0.95,
          pane: "stopsPane",
        };
      } else if (seq !== null && maxSeq !== null && seq === maxSeq) {
        // Last stop: red
        markerOptions = {
          radius: 7,
          color: "#ef4444",
          weight: 2,
          fillColor: "#fecaca",
          fillOpacity: 0.95,
          pane: "stopsPane",
        };
      }

      const marker = stopIcon([lat, lon], markerOptions).addTo(
        state.layers.routeStops
      );

      const stop = state.stops.find((s) => s.stop_id === props.stop_id);
      const name = (stop && stop.name) || props.name || props.stop_id;

      const popupDiv = document.createElement("div");
      popupDiv.innerHTML = `
        <div class="stop-popup-title">${name}</div>
        <div class="stop-popup-meta">
          ID: ${props.stop_id}${seq !== null ? ` &middot; Seq: ${seq}` : ""}
        </div>
      `;

      marker.bindPopup(popupDiv);
    } else if (geomType === "LineString") {
      const coords = f.geometry.coordinates.map((c) => [c[1], c[0]]);
      state._routeLineCoords.push(coords);
      const triangleOpts = { ...DIRECTION_TRIANGLE_OPTS, map: state.map };
      if (props.kind === "pattern_snapped") {
        const l = L.polyline(coords, snappedStyle).addTo(
          state.layers.snappedPattern
        );
        edgeLayers.push(l);
        addDirectionTrianglesAlongLine(coords, state.layers.directionTriangles, triangleOpts);
      } else {
        const l = L.polyline(coords, edgeStyle).addTo(state.layers.routeEdges);
        edgeLayers.push(l);
        addDirectionTrianglesAlongLine(coords, state.layers.directionTriangles, triangleOpts);
      }
    }
  });

  if (edgeLayers.length > 0) {
    const group = L.featureGroup(edgeLayers);
    state._lastRouteBounds = group.getBounds();
    // Don't auto-zoom to route when user has a blockage drawn — keeps polygon in view.
    if (!state.areaGeoJSON) {
      map.fitBounds(state._lastRouteBounds.pad(0.15));
    }
    // Rebuild triangles at current zoom (important after fitBounds may have changed zoom).
    redrawDirectionTriangles();
  }

  // If no explicit start/end were chosen yet, default to first/last stop of the pattern.
  if (!state.startStopId && minSeq !== null) {
    const firstStopEntry = Object.entries(seqByStopId).find(
      ([, seq]) => seq === minSeq
    );
    if (firstStopEntry) {
      state.startStopId = firstStopEntry[0];
    }
  }
  if (!state.endStopId && maxSeq !== null) {
    const lastStopEntry = Object.entries(seqByStopId).find(
      ([, seq]) => seq === maxSeq
    );
    if (lastStopEntry) {
      state.endStopId = lastStopEntry[0];
    }
  }
  updateSelectedStopsSummary();
  document.getElementById("btn-compute-detour").disabled = !state.areaGeoJSON;

  if (state.layers.routeEdges) state.layers.routeEdges.bringToBack();
  if (state.layers.routeStops) state.layers.routeStops.bringToFront();
  if (state.layers.directionTriangles) state.layers.directionTriangles.bringToFront();
}

async function handleRouteSearch() {
  const q = document.getElementById("search-q").value.trim();
  if (!q) {
    showToast("Enter a route query (e.g. 5, 480, חיפה).", { error: true });
    return;
  }
  const resultsEl = document.getElementById("search-results");
  resultsEl.innerHTML = "<div class='result-item'><em>Searching…</em></div>";

  try {
    const data = await api("/routes/search", {
      method: "POST",
      body: JSON.stringify({ q, limit: 30 }),
    });
    if (!Array.isArray(data) || data.length === 0) {
      resultsEl.innerHTML =
        "<div class='result-item'><em>No routes found.</em></div>";
      return;
    }
    resultsEl.innerHTML = "";
    data.forEach((r) => {
      const div = document.createElement("div");
      div.className = "result-item";
      div.dataset.routeId = r.route_id;
      div.innerHTML = `
        <span class="route-id">${r.route_short_name || r.route_id}</span>
        <span class="route-name">${
          r.route_long_name || r.route_id || ""
        }</span>
      `;
      div.onclick = () => {
        Array.from(resultsEl.children).forEach((c) =>
          c.classList.remove("selected")
        );
        div.classList.add("selected");
        state.selectedRoute = r;
        document.getElementById("route-id").value = r.route_id;
        showToast(`Selected route ${r.route_short_name || r.route_id}.`, {
          timeout: 2000,
        });
      };
      resultsEl.appendChild(div);
    });
  } catch (err) {
    console.error(err);
    resultsEl.innerHTML =
      "<div class='result-item'><em>Error while searching.</em></div>";
    showToast(err.message || "Route search failed.", { error: true });
  }
}

async function handleBuildGraph() {
  if (!state.selectedRoute) {
    showToast("Select a route from the search results first.", {
      error: true,
    });
    return;
  }
  const dir = document.getElementById("direction-id").value || null;
  const dateInput = document.getElementById("route-date").value;
  const pretty = document.getElementById("pretty-osm").checked;
  const ymd = ymdFromDateInput(dateInput);

  if (!ymd) {
    showToast("Please pick a service date.", { error: true });
    return;
  }

  state.directionId = dir;
  state.dateYMD = ymd;
  state.prettyOsm = pretty;

  const graphSummaryEl = document.getElementById("graph-summary");
  graphSummaryEl.innerHTML = "<em>Building graph…</em>";

  try {
    const buildReq = {
      route_id: state.selectedRoute.route_id,
      direction_id: dir,
      date: ymd,
      max_trips: 50,
      pretty_osm: pretty,
    };
    const buildRes = await api("/graph/build", {
      method: "POST",
      body: JSON.stringify(buildReq),
    });

    state.patternId = buildRes.pattern_id;

    graphSummaryEl.innerHTML = `
      <div class="summary-row">
        <span><strong>Pattern</strong>: ${buildRes.pattern_id}</span>
      </div>
      <div class="summary-row" style="margin-top:4px;">
        <span><strong>Stops</strong>: ${buildRes.stop_count}</span>
        <span><strong>Edges</strong>: ${buildRes.edge_count}</span>
      </div>
      <div style="margin-top:4px;">
        <span><strong>Shape</strong>: ${
          buildRes.used_shape ? "GTFS shapes.txt" : "straight lines"
        }</span><br/>
        <span><strong>OSM snapping</strong>: ${
          buildRes.used_osm_snapping ? "yes" : "no"
        }</span>
      </div>
    `;

    const stopsRes = await api(
      `/graph/stops?route_id=${encodeURIComponent(
        state.selectedRoute.route_id
      )}&direction_id=${encodeURIComponent(
        dir || ""
      )}&pattern_id=&date=${encodeURIComponent(ymd)}`
    );
    state.stops = stopsRes.stops || [];
    state.patternId = stopsRes.pattern_id || buildRes.pattern_id;
    updateStopsSummaryBox();

    const geojsonRes = await api(
      `/graph/geojson?route_id=${encodeURIComponent(
        state.selectedRoute.route_id
      )}&direction_id=${encodeURIComponent(
        dir || ""
      )}&pattern_id=${encodeURIComponent(
        state.patternId || ""
      )}&date=${encodeURIComponent(ymd)}&pretty_osm=${
        pretty ? "true" : "false"
      }`
    );
    renderRouteGeoJSON(geojsonRes);

    state.startStopId = null;
    state.endStopId = null;
    state.areaGeoJSON = state.areaGeoJSON || null;
    updateSelectedStopsSummary();
    document.getElementById("btn-compute-detour").disabled = !state.areaGeoJSON;
    state.layers.blockage.clearLayers();
    state.layers.detour.clearLayers();

    showToast("Graph built and route rendered on the map.", { timeout: 2500 });
  } catch (err) {
    console.error(err);
    graphSummaryEl.innerHTML =
      "<em>Error building graph. Check console for details.</em>";
    showToast(err.message || "Graph build failed.", { error: true });
  }
}

async function handleComputeDetour() {
  if (!state.areaGeoJSON) {
    showToast("Draw an area/blockage polygon on the map first.", {
      error: true,
    });
    return;
  }

  const dateInput = document.getElementById("route-date").value;
  const dateYMD = ymdFromDateInput(dateInput);
  if (!dateYMD) {
    showToast("Please pick a service date.", { error: true });
    return;
  }
  const timeStart = document.getElementById("time-start").value || "04:00";
  const timeEnd = document.getElementById("time-end").value || "23:59";

  // Polygon-only detour: route-only if we have a built route, else all affected lines.
  const mode =
    state.selectedRoute && state.patternId ? "route" : "all";
  const req = {
    mode,
    route_id: state.selectedRoute ? state.selectedRoute.route_id : null,
    direction_id: state.directionId || null,
    date: dateYMD,
    start_time: timeStart,
    end_time: timeEnd,
    blockage_geojson: state.areaGeoJSON,
    max_routes: 20,
    transfer_radius_m: 120,
  };

  const detourSummaryEl = document.getElementById("detour-summary");
  detourSummaryEl.innerHTML = "<em>Computing detour…</em>";

  try {
    const res = await api("/detours/by-area", {
      method: "POST",
      body: JSON.stringify(req),
    });

    state.layers.detour.clearLayers();
    let geojsonToDraw = null;

    if (mode === "route" && res.result) {
      detourSummaryEl.innerHTML = formatDetourSummary(res.result, mode);
      if (res.result.detour_geojson && res.result.detour_geojson.type === "FeatureCollection") {
        geojsonToDraw = res.result.detour_geojson;
      } else if (res.result.detour_geojson && Array.isArray(res.result.detour_geojson.features)) {
        geojsonToDraw = { type: "FeatureCollection", features: res.result.detour_geojson.features };
      }
    } else if (res.results && res.results.length) {
      detourSummaryEl.innerHTML = formatDetourSummaryAll(res.results);
      const features = [];
      res.results.forEach((r) => {
        if (r.detour_geojson) {
          if (Array.isArray(r.detour_geojson.features)) {
            features.push(...r.detour_geojson.features);
          } else if (r.detour_geojson.type === "Feature") {
            features.push(r.detour_geojson);
          }
        }
      });
      if (features.length) {
        geojsonToDraw = { type: "FeatureCollection", features };
      }
    } else {
      detourSummaryEl.innerHTML =
        "<em>No detours returned. Try a different time window or area.</em>";
    }

    if (geojsonToDraw && geojsonToDraw.features && geojsonToDraw.features.length > 0) {
      const detourStyle = {
        color: "#eab308",
        weight: 6,
        opacity: 0.95,
        fill: false,
        pane: "detourPane",
      };
      const detourLayer = L.geoJSON(geojsonToDraw, {
        style: detourStyle,
      });
      detourLayer.addTo(state.layers.detour);
      state.layers.detour.bringToFront();
      try {
        const bounds = detourLayer.getBounds();
        if (bounds.isValid()) {
          state.map.fitBounds(bounds.pad(0.15));
        }
      } catch (e) {
        // ignore
      }
      showToast("Detour(s) drawn in yellow. Summary below.", { timeout: 3000 });
    }
  } catch (err) {
    console.error(err);
    detourSummaryEl.innerHTML =
      "<em>Error computing detour. Check console for details.</em>";
    showToast(err.message || "Detour computation failed.", { error: true });
  }
}

function formatDetourSummary(result, mode) {
  const blocked = result.blocked_edges_count ?? "?";
  const err = result.error ? `<div class="error">${result.error}</div>` : "";
  return `
    <div><strong>Blocked edges</strong>: ${blocked}</div>
    <div><strong>Route</strong>: ${result.route_id}${result.direction_id != null ? " (dir " + result.direction_id + ")" : ""}</div>
    <div>Stop before → after: ${result.stop_before || "—"} → ${result.stop_after || "—"}</div>
    <div>Used transfers: ${result.used_transfers ? "Yes" : "No"}</div>
    ${err}
  `;
}

function formatDetourSummaryAll(results) {
  const withDetour = results.filter((r) => r.detour_geojson && !r.error);
  const failed = results.filter((r) => r.error);
  let html = `<div><strong>${withDetour.length}</strong> detour(s) found`;
  if (failed.length) html += `, <strong>${failed.length}</strong> failed`;
  html += ".</div>";
  withDetour.slice(0, 5).forEach((r) => {
    html += `<div class="summary-row">${r.route_id}: ${r.blocked_edges_count} blocked, ${r.used_transfers ? "with transfers" : "no transfers"}</div>`;
  });
  if (failed.length) {
    failed.slice(0, 3).forEach((r) => {
      html += `<div class="error">${r.route_id}: ${r.error || "no path"}</div>`;
    });
  }
  return html;
}

function handleClearBlockage() {
  state.areaGeoJSON = null;
  if (state.layers.blockage) state.layers.blockage.clearLayers();
  if (state.layers.detour) state.layers.detour.clearLayers();
  document.getElementById("btn-compute-detour").disabled = true;
  const detourSummaryEl = document.getElementById("detour-summary");
  detourSummaryEl.innerHTML = "<em>No detour computed yet.</em>";
}

async function handleAreaRoutesSearch() {
  if (!state.areaGeoJSON) {
    showToast("Draw a polygon/line on the map to define the area first.", {
      error: true,
    });
    return;
  }

  const dateInput = document.getElementById("route-date").value;
  const ymd = ymdFromDateInput(dateInput);
  if (!ymd) {
    showToast("Please pick a service date (used for area search).", {
      error: true,
    });
    return;
  }

  const timeStart = document.getElementById("time-start").value || "00:00";
  const timeEnd = document.getElementById("time-end").value || "23:59";

  const areaResultsEl = document.getElementById("area-routes-results");
  areaResultsEl.innerHTML =
    "<div class='result-item'><em>Searching lines in area…</em></div>";

  try {
    const req = {
      date: ymd,
      start_time: timeStart,
      end_time: timeEnd,
      polygon_geojson: state.areaGeoJSON,
      max_results: 200,
    };
    const res = await api("/area/routes", {
      method: "POST",
      body: JSON.stringify(req),
    });
    const routes = res.routes || [];
    state.areaRoutes = routes;

    if (!routes.length) {
      areaResultsEl.innerHTML =
        "<div class='result-item'><em>No lines found in this area/time window.</em></div>";
      return;
    }

    areaResultsEl.innerHTML = "";
    routes.forEach((r) => {
      const div = document.createElement("div");
      div.className = "result-item";
      div.innerHTML = `
        <span class="route-id">${
          r.route_short_name || r.route_id
        }${r.direction_id !== null && r.direction_id !== undefined && r.direction_id !== ""
          ? ` (dir ${r.direction_id})`
          : ""
        }</span>
        <span class="route-name">${
          r.route_long_name || r.route_id || ""
        }</span>
        <span class="hint">
          ${r.first_time || "??:??"}–${r.last_time || "??:??"}
        </span>
      `;

      div.onclick = async () => {
        // When clicking a line from area results, set it as selected
        Array.from(areaResultsEl.children).forEach((c) =>
          c.classList.remove("selected")
        );
        div.classList.add("selected");

        // Hydrate selectedRoute minimally; route metadata will still come from /routes/search if desired.
        state.selectedRoute = {
          route_id: r.route_id,
          route_short_name: r.route_short_name,
          route_long_name: r.route_long_name,
          agency_id: r.agency_id,
        };
        document.getElementById("route-id").value = r.route_id;
        document.getElementById("direction-id").value =
          r.direction_id != null ? String(r.direction_id) : "";

        showToast(
          `Selected line ${r.route_short_name || r.route_id} from area search. Building graph…`,
          { timeout: 2500 }
        );

        // Automatically build graph for this route/direction/date using the same settings
        await handleBuildGraph();
      };

      areaResultsEl.appendChild(div);
    });
  } catch (err) {
    console.error(err);
    areaResultsEl.innerHTML =
      "<div class='result-item'><em>Error while searching lines in area.</em></div>";
    showToast(err.message || "Area lines search failed.", { error: true });
  }
}

function handleFitToRoute() {
  const map = state.map;
  if (!map) return;
  if (state._lastRouteBounds && state._lastRouteBounds.isValid()) {
    map.fitBounds(state._lastRouteBounds.pad(0.15));
    showToast("Fitted map to route.", { timeout: 1500 });
  } else {
    const group = L.featureGroup([state.layers.routeEdges, state.layers.directionTriangles]);
    if (group.getBounds().isValid()) {
      map.fitBounds(group.getBounds().pad(0.15));
      showToast("Fitted map to route.", { timeout: 1500 });
    } else {
      showToast("No route on map. Build a graph first.", { error: true });
    }
  }
}

function handleFitToBlockage() {
  const map = state.map;
  if (!map || !state.layers.blockage) return;
  const layers = state.layers.blockage.getLayers();
  if (layers.length === 0) {
    showToast("No blockage drawn. Draw a polygon first.", { error: true });
    return;
  }
  const group = L.featureGroup(layers);
  if (group.getBounds().isValid()) {
    map.fitBounds(group.getBounds().pad(0.2));
    showToast("Fitted map to blockage.", { timeout: 1500 });
  }
}

function initUI() {
  const today = new Date();
  const yyyy = today.getFullYear();
  const mm = String(today.getMonth() + 1).padStart(2, "0");
  const dd = String(today.getDate()).padStart(2, "0");
  const dateInput = document.getElementById("route-date");
  dateInput.value = `${yyyy}-${mm}-${dd}`;

  document
    .getElementById("btn-search-route")
    .addEventListener("click", handleRouteSearch);
  document
    .getElementById("btn-build-graph")
    .addEventListener("click", handleBuildGraph);
  document
    .getElementById("btn-area-routes")
    .addEventListener("click", handleAreaRoutesSearch);
  document
    .getElementById("btn-compute-detour")
    .addEventListener("click", handleComputeDetour);
  document
    .getElementById("btn-fit-route")
    .addEventListener("click", handleFitToRoute);
  document
    .getElementById("btn-fit-blockage")
    .addEventListener("click", handleFitToBlockage);
  document
    .getElementById("btn-clear-blockage")
    .addEventListener("click", handleClearBlockage);

  document.getElementById("graph-summary").innerHTML =
    "<em>No graph built yet.</em>";
  document.getElementById("stops-summary").innerHTML =
    "<em>No stops loaded yet.</em>";
  document.getElementById("detour-summary").innerHTML =
    "<em>No detour computed yet.</em>";
  updateSelectedStopsSummary();
}

window.addEventListener("DOMContentLoaded", () => {
  initMap();
  initUI();
  showToast("Backend detected at http://127.0.0.1:8000. Start by searching for a route.", {
    timeout: 3500,
  });
});


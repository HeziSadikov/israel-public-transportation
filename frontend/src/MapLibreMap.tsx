import "maplibre-gl/dist/maplibre-gl.css";
import "@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css";

import React, { useEffect, useImperativeHandle, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import MapboxDraw from "@mapbox/mapbox-gl-draw";

// GeoJSON types for internal use (coordinates as [lng, lat])
type GeoJSONPosition = [number, number];
type GeoJSONGeometry =
  | { type: "Polygon"; coordinates: GeoJSONPosition[][] }
  | { type: "LineString"; coordinates: GeoJSONPosition[] }
  | { type: "Point"; coordinates: GeoJSONPosition };
type GeoJSONFeature = { type: "Feature"; geometry: GeoJSONGeometry; properties?: Record<string, unknown> };
type GeoJSONFeatureCollection = { type: "FeatureCollection"; features: GeoJSONFeature[] };

/** Shared paint: sharper scaling + slightly subdued basemap so routes/detours read clearer (GPU-only). */
const RASTER_BASEMAP_PAINT: maplibregl.RasterLayerSpecification["paint"] = {
  /** Avoid cross-fade between old/new tiles (default ~300ms can look soft while zooming). */
  "raster-fade-duration": 0,
  "raster-resampling": "linear",
  "raster-opacity": 0.96,
  "raster-brightness-min": 0,
  "raster-brightness-max": 1,
  "raster-saturation": -0.12,
  "raster-contrast": 0.06,
};

export type BasemapKind = "osm" | "carto_light" | "vector_liberty";
const BASEMAP_RASTER_SOURCE = "basemap-raster-source";
const BASEMAP_RASTER_LAYER = "basemap-raster";

/** OpenFreeMap Liberty — vector tiles, crisp at all zooms (separate CDN; check terms for production). */
const OPENFREEMAP_LIBERTY_STYLE = "https://tiles.openfreemap.org/styles/liberty";

function rasterStyle(tiles: string[], attribution: string): maplibregl.StyleSpecification {
  return {
    version: 8,
    sources: {
      [BASEMAP_RASTER_SOURCE]: {
        type: "raster",
        tiles,
        tileSize: 256,
        attribution,
        maxzoom: 19,
      },
    },
    layers: [
      {
        id: BASEMAP_RASTER_LAYER,
        type: "raster",
        source: BASEMAP_RASTER_SOURCE,
        paint: RASTER_BASEMAP_PAINT,
      },
    ],
  };
}

const OSM_STYLE = rasterStyle(["https://tile.openstreetmap.org/{z}/{x}/{y}.png"], "© OpenStreetMap");

/** Carto Positron-style light raster — less visual noise than full OSM. */
const CARTO_LIGHT_STYLE = rasterStyle(
  [
    "https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
    "https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
    "https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
  ],
  "© OpenStreetMap contributors © CARTO"
);

function styleForBasemap(kind: BasemapKind): string | maplibregl.StyleSpecification {
  if (kind === "vector_liberty") return OPENFREEMAP_LIBERTY_STYLE;
  if (kind === "carto_light") return CARTO_LIGHT_STYLE;
  return OSM_STYLE;
}

function isRasterBasemap(kind: BasemapKind): boolean {
  return kind === "osm" || kind === "carto_light";
}

function rasterTilesForBasemap(kind: BasemapKind): string[] {
  if (kind === "carto_light") {
    return [
      "https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
      "https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
      "https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
    ];
  }
  return ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"];
}

/**
 * Raster tiles (OSM/Carto) are fixed 256px; MapLibre upscales to the canvas.
 * Use full devicePixelRatio so HiDPI screens look sharp (more GPU/tile work on 3x).
 * Vector basemap: cap at 2 to limit label/GPU cost on very dense 3x phones.
 */
function mapPixelRatioForBasemap(kind: BasemapKind): number {
  const dpr = typeof window !== "undefined" ? window.devicePixelRatio || 1 : 1;
  if (kind === "vector_liberty") return Math.min(dpr, 2);
  return dpr;
}

// Custom, minimal styles for MapboxDraw that are compatible with MapLibre.
// We avoid any complex line-dasharray expressions that trigger style errors.
const DRAW_STYLES: any[] = [
  // Inactive polygon fill
  {
    id: "gl-draw-polygon-fill-inactive",
    type: "fill",
    filter: ["all", ["==", "active", "false"], ["==", "$type", "Polygon"]],
    paint: {
      "fill-color": "#f97316",
      "fill-outline-color": "#dc2626",
      "fill-opacity": 0.2,
    },
  },
  // Active polygon fill
  {
    id: "gl-draw-polygon-fill-active",
    type: "fill",
    filter: ["all", ["==", "active", "true"], ["==", "$type", "Polygon"]],
    paint: {
      "fill-color": "#f97316",
      "fill-outline-color": "#dc2626",
      "fill-opacity": 0.4,
    },
  },
  // Active polygon outline
  {
    id: "gl-draw-polygon-stroke-active",
    type: "line",
    filter: ["all", ["==", "active", "true"], ["==", "$type", "Polygon"]],
    layout: {
      "line-cap": "round",
      "line-join": "round",
    },
    paint: {
      "line-color": "#dc2626",
      "line-width": 2,
    },
  },
  // Inactive polygon outline
  {
    id: "gl-draw-polygon-stroke-inactive",
    type: "line",
    filter: ["all", ["==", "active", "false"], ["==", "$type", "Polygon"]],
    layout: {
      "line-cap": "round",
      "line-join": "round",
    },
    paint: {
      "line-color": "#dc2626",
      "line-width": 2,
    },
  },
  // Vertex points
  {
    id: "gl-draw-polygon-vertex",
    type: "circle",
    filter: ["all", ["==", "$type", "Point"], ["==", "meta", "vertex"]],
    paint: {
      "circle-radius": 4,
      "circle-color": "#ffffff",
      "circle-stroke-width": 2,
      "circle-stroke-color": "#dc2626",
    },
  },
  // Edge midpoints (direct_select: add vertices along edges)
  {
    id: "gl-draw-midpoint",
    type: "circle",
    filter: ["all", ["==", "meta", "midpoint"]],
    paint: {
      "circle-radius": 3,
      "circle-color": "#f97316",
      "circle-stroke-width": 1,
      "circle-stroke-color": "#dc2626",
    },
  },
];

const SOURCE_ROUTE = "route";
const SOURCE_DETOUR = "detour";
const SOURCE_STOPS = "stops";
const SOURCE_PIN = "pin";
/** First-edge rubber line while drawing (one anchor + cursor); not part of MapboxDraw HOT/COLD. */
const SOURCE_POLYGON_PREVIEW = "blockage-polygon-preview-line";
const LAYER_POLYGON_PREVIEW = "blockage-polygon-preview-line-layer";

const EMPTY_FC: GeoJSONFeatureCollection = { type: "FeatureCollection", features: [] };

/** MapboxDraw duplicates each style for cold/hot sources; layer ids are `${DRAW_STYLES.id}.hot`. */
const DRAW_VERTEX_LAYER_HOT = "gl-draw-polygon-vertex.hot";

/** Normalize drawn geometry: close polygon ring if needed (GeoJSON spec; backend expects closed). */
function normalizeBlockageGeometry(geom: GeoJSON.Geometry): GeoJSON.Geometry {
  if (geom.type === "Polygon" && geom.coordinates?.[0]?.length) {
    const ring = geom.coordinates[0];
    const first = ring[0];
    const last = ring[ring.length - 1];
    const closed =
      first &&
      last &&
      first.length >= 2 &&
      last.length >= 2 &&
      first[0] === last[0] &&
      first[1] === last[1];
    if (!closed && ring.length >= 2) {
      return {
        type: "Polygon",
        coordinates: [[...ring, [ring[0][0], ring[0][1]]]],
      };
    }
  }
  if (geom.type === "MultiPolygon" && geom.coordinates?.length) {
    return {
      type: "MultiPolygon",
      coordinates: geom.coordinates.map((poly) => {
        const ring = poly[0];
        if (!ring?.length) return poly;
        const first = ring[0] as number[];
        const last = ring[ring.length - 1] as number[];
        const closed = first && last && first[0] === last[0] && first[1] === last[1];
        if (!closed && ring.length >= 2) return [[...ring, [ring[0][0], ring[0][1]]]];
        return poly;
      }),
    };
  }
  return geom;
}

function firstPolygonFeatureIdFromDrawAll(all: GeoJSON.FeatureCollection): string | null {
  const f = all.features?.find((x) => x.geometry?.type === "Polygon");
  const id = f?.id;
  if (id === undefined || id === null) return null;
  return String(id);
}

export type MapLibreMapHandle = {
  clearBlockage: () => void;
  cancelDrawing: () => void;
  undoLastPoint: () => void;
  startPolygon: () => void;
  editBlockagePolygon: () => void;
  fitToBlockage: () => void;
  fitToRoute: () => void;
  fitToDetour: () => void;
  flyTo: (lat: number, lng: number, zoom?: number) => void;
};

export type MapLibreMapProps = {
  /** Center as [lat, lng] (converted to [lng, lat] for MapLibre) */
  center: [number, number];
  stops: { stop_id: string; name: string; stop_code?: string | null; lat: number; lon: number; sequence: number }[];
  routeGeojson: GeoJSON.FeatureCollection | null;
  detour: { path_geojson?: GeoJSON.FeatureCollection | null } | null;
  blockageGeojson: GeoJSON.Geometry | null;
  onBlockageChange: (geom: GeoJSON.Geometry | null) => void;
  pinPosition: [number, number] | null;
  selectedStopId?: string | null;
  onStopClick?: (stop: { stop_id: string; stop_name?: string; stop_code?: string | null; lat: number; lon: number }) => void;
  onStopOpenInExplorer?: (stop: {
    stop_id: string;
    stop_name?: string;
    stop_code?: string | null;
    lat: number;
    lon: number;
  }) => void;
  /** Basemap: OSM raster, minimal Carto raster, or vector (OpenFreeMap Liberty). */
  basemap?: BasemapKind;
  /** Fired when MapboxDraw mode changes (e.g. draw_polygon, direct_select, simple_select). */
  onDrawModeChange?: (mode: string) => void;
};

type StopRole = "first" | "last" | "middle" | "both";

function rolesForStops(stops: MapLibreMapProps["stops"]): Map<string, StopRole> {
  const sequences = stops.map((s) => s.sequence).filter((n) => Number.isFinite(n));
  let minSeq: number | null = null;
  let maxSeq: number | null = null;
  if (sequences.length > 0) {
    minSeq = Math.min(...sequences);
    maxSeq = Math.max(...sequences);
  }
  const byId = new Map<string, StopRole>();
  for (const s of stops) {
    const seq = s.sequence;
    if (!Number.isFinite(seq) || minSeq === null || maxSeq === null) {
      byId.set(s.stop_id, "middle");
      continue;
    }
    if (minSeq === maxSeq) {
      byId.set(s.stop_id, "both");
    } else if (seq === minSeq) {
      byId.set(s.stop_id, "first");
    } else if (seq === maxSeq) {
      byId.set(s.stop_id, "last");
    } else {
      byId.set(s.stop_id, "middle");
    }
  }
  return byId;
}

const MapLibreMap = React.forwardRef<MapLibreMapHandle, MapLibreMapProps>(function MapLibreMap(
  {
    center,
    stops,
    routeGeojson,
    detour,
    blockageGeojson,
    onBlockageChange,
    pinPosition,
    selectedStopId = null,
    onStopClick,
    onStopOpenInExplorer,
    basemap = "osm",
    onDrawModeChange,
  },
  ref
) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const drawRef = useRef<MapboxDraw | null>(null);
  const blockageRef = useRef<GeoJSON.Geometry | null>(null);
  const routeRef = useRef<GeoJSON.FeatureCollection | null>(null);
  const detourRef = useRef<MapLibreMapProps["detour"]>(null);
  const stopsRef = useRef<MapLibreMapProps["stops"]>([]);
  const selectedStopIdRef = useRef<string | null>(null);
  const pinPositionRef = useRef<[number, number] | null>(null);
  const blockagePropRef = useRef<GeoJSON.Geometry | null>(null);
  const onStopClickRef = useRef<MapLibreMapProps["onStopClick"]>(undefined);
  const onStopOpenInExplorerRef = useRef<MapLibreMapProps["onStopOpenInExplorer"]>(undefined);
  const onDrawModeChangeRef = useRef<MapLibreMapProps["onDrawModeChange"]>(undefined);
  const stopPopupRef = useRef<maplibregl.Popup | null>(null);
  const stopLayerClickRef = useRef<((e: maplibregl.MapMouseEvent & maplibregl.EventData) => void) | null>(null);
  const stopLayerMouseEnterRef = useRef<(() => void) | null>(null);
  const stopLayerMouseLeaveRef = useRef<(() => void) | null>(null);
  const drawDetachRef = useRef<(() => void) | null>(null);
  const styleRehydrateRef = useRef<(() => void) | null>(null);
  const basemapRef = useRef<BasemapKind>(basemap);
  const initialBasemapRef = useRef<BasemapKind>(basemap);
  const [mapReady, setMapReady] = useState(false);

  blockageRef.current = blockageGeojson;
  blockagePropRef.current = blockageGeojson;
  routeRef.current = routeGeojson;
  detourRef.current = detour;
  stopsRef.current = stops;
  selectedStopIdRef.current = selectedStopId;
  pinPositionRef.current = pinPosition;
  onStopClickRef.current = onStopClick;
  onStopOpenInExplorerRef.current = onStopOpenInExplorer;
  onDrawModeChangeRef.current = onDrawModeChange;

  useEffect(() => {
    if (!containerRef.current) return;
    const [lat, lng] = center;
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: styleForBasemap(initialBasemapRef.current),
      center: [lng, lat],
      zoom: 9,
      minZoom: 5,
      maxZoom: 19,
      dragRotate: false,
      pitchWithRotate: false,
      pixelRatio: mapPixelRatioForBasemap(initialBasemapRef.current),
    });
    // Make mouse-wheel zoom a bit faster/more responsive.
    map.scrollZoom.setWheelZoomRate(1 / 180);
    map.scrollZoom.setZoomRate(1.0);
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    mapRef.current = map;

    const containerEl = containerRef.current;
    const scheduleResize = () => {
      map.resize();
    };
    const resizeObserver =
      typeof ResizeObserver !== "undefined" && containerEl
        ? new ResizeObserver(() => {
            scheduleResize();
          })
        : null;
    resizeObserver?.observe(containerEl);
    window.addEventListener("resize", scheduleResize);
    requestAnimationFrame(scheduleResize);

    const closeStopPopup = () => {
      stopPopupRef.current?.remove();
      stopPopupRef.current = null;
    };
    const openStopPopup = (lng: number, lat: number, stopName: string, stopCode: string | null, stopId: string) => {
      closeStopPopup();
      const stopPayload = {
        stop_id: stopId,
        stop_name: stopName || undefined,
        stop_code: stopCode,
        lat,
        lon: lng,
      };
      const content = document.createElement("div");
      content.className = "stop-map-popup";
      const title = document.createElement("div");
      title.className = "stop-map-popup-title";
      title.textContent = stopName || "Unnamed stop";
      const codeRow = document.createElement("div");
      codeRow.className = "stop-map-popup-meta";
      codeRow.textContent = `Code: ${stopCode || "N/A"}`;
      const idRow = document.createElement("div");
      idRow.className = "stop-map-popup-meta";
      idRow.textContent = `ID: ${stopId}`;
      const actionBtn = document.createElement("button");
      actionBtn.className = "stop-map-popup-action";
      actionBtn.type = "button";
      actionBtn.textContent = "Open in Stop tab";
      actionBtn.onclick = (event) => {
        event.stopPropagation();
        onStopOpenInExplorerRef.current?.(stopPayload);
      };
      content.appendChild(title);
      content.appendChild(codeRow);
      content.appendChild(idRow);
      content.appendChild(actionBtn);
      stopPopupRef.current = new maplibregl.Popup({
        closeButton: true,
        closeOnClick: false,
        className: "stop-map-popup-frame",
        offset: 14,
        maxWidth: "260px",
      })
        .setLngLat([lng, lat])
        .setDOMContent(content)
        .addTo(map);
    };

    const ensureCoreSourcesAndLayers = () => {
      // Persistent sources + layers (created once)
      const emptyFc: GeoJSONFeatureCollection = { type: "FeatureCollection", features: [] };

      if (!map.getSource(SOURCE_ROUTE)) {
        map.addSource(SOURCE_ROUTE, { type: "geojson", data: emptyFc });
        map.addLayer({
          id: "route-line",
          type: "line",
          source: SOURCE_ROUTE,
          paint: { "line-color": "#2563eb", "line-width": 4 },
        });
      }

      if (!map.getSource(SOURCE_DETOUR)) {
        map.addSource(SOURCE_DETOUR, { type: "geojson", data: emptyFc });
        map.addLayer({
          id: "detour-line",
          type: "line",
          source: SOURCE_DETOUR,
          paint: { "line-color": "#16a34a", "line-width": 6 },
        });
      }

      if (!map.getSource(SOURCE_STOPS)) {
        map.addSource(SOURCE_STOPS, { type: "geojson", data: emptyFc });
        map.addLayer({
          id: "stops-circles",
          type: "circle",
          source: SOURCE_STOPS,
          paint: {
            "circle-radius": [
              "case",
              ["boolean", ["get", "is_selected"], false],
              9,
              ["match", ["get", "role"], "first", 7, "last", 7, "both", 8, 6],
            ],
            "circle-color": [
              "case",
              ["boolean", ["get", "is_selected"], false],
              "#f59e0b",
              ["match", ["get", "role"], "first", "#bbf7d0", "last", "#fecaca", "both", "#e9d5ff", "#1e40af"],
            ],
            "circle-stroke-width": [
              "case",
              ["boolean", ["get", "is_selected"], false],
              3,
              2,
            ],
            "circle-stroke-color": [
              "case",
              ["boolean", ["get", "is_selected"], false],
              "#ffffff",
              ["match", ["get", "role"], "first", "#22c55e", "last", "#ef4444", "both", "#7c3aed", "#fff"],
            ],
          },
        });
      }

      if (stopLayerClickRef.current) {
        map.off("click", "stops-circles", stopLayerClickRef.current);
        stopLayerClickRef.current = null;
      }
      if (stopLayerMouseEnterRef.current) {
        map.off("mouseenter", "stops-circles", stopLayerMouseEnterRef.current);
        stopLayerMouseEnterRef.current = null;
      }
      if (stopLayerMouseLeaveRef.current) {
        map.off("mouseleave", "stops-circles", stopLayerMouseLeaveRef.current);
        stopLayerMouseLeaveRef.current = null;
      }
      const onStopLayerClick = (e: maplibregl.MapMouseEvent & maplibregl.EventData) => {
        const f = e.features?.[0];
        const p = (f?.properties || {}) as Record<string, unknown>;
        const geom = f?.geometry;
        if (!geom || geom.type !== "Point") return;
        const coords = geom.coordinates as number[];
        if (!Array.isArray(coords) || coords.length < 2) return;
        const stopId = typeof p.stop_id === "string" ? p.stop_id : "";
        if (!stopId) return;
        const stopName = typeof p.stop_name === "string" ? p.stop_name : "";
        const stopCode = typeof p.stop_code === "string" ? p.stop_code : null;
        openStopPopup(Number(coords[0]), Number(coords[1]), stopName, stopCode, stopId);
        onStopClickRef.current?.({
          stop_id: stopId,
          stop_name: stopName || undefined,
          stop_code: stopCode,
          lat: Number(coords[1]),
          lon: Number(coords[0]),
        });
      };
      const onStopMouseEnter = () => {
        map.getCanvas().style.cursor = "pointer";
      };
      const onStopMouseLeave = () => {
        map.getCanvas().style.cursor = "";
      };
      stopLayerClickRef.current = onStopLayerClick;
      stopLayerMouseEnterRef.current = onStopMouseEnter;
      stopLayerMouseLeaveRef.current = onStopMouseLeave;
      map.on("click", "stops-circles", onStopLayerClick);
      map.on("mouseenter", "stops-circles", onStopMouseEnter);
      map.on("mouseleave", "stops-circles", onStopMouseLeave);

      if (!map.getSource(SOURCE_PIN)) {
        map.addSource(SOURCE_PIN, { type: "geojson", data: emptyFc });
        map.addLayer({
          id: "pin-marker",
          type: "circle",
          source: SOURCE_PIN,
          paint: {
            "circle-radius": 10,
            "circle-color": "#e11d48",
            "circle-stroke-width": 2,
            "circle-stroke-color": "#fff",
          },
        });
      }

      if (!drawRef.current) {
        // Draw control: polygon only (explicitly activated by the toolbar button)
        const draw = new MapboxDraw({
          displayControlsDefault: false,
          controls: {
            polygon: true,
            trash: true,
          },
          styles: DRAW_STYLES,
        });
        // Place controls in the top-right to avoid overlap with the explorer window.
        map.addControl(draw as any, "top-right");
        drawRef.current = draw;

        const drawApi = draw as MapboxDraw & { getMode?: () => string };
        const notifyDrawMode = () => {
          if (typeof drawApi.getMode !== "function") return;
          onDrawModeChangeRef.current?.(drawApi.getMode()!);
        };
        const syncPolygonPreviewLine = () => {
          const src = map.getSource(SOURCE_POLYGON_PREVIEW) as maplibregl.GeoJSONSource | undefined;
          if (!src) return;
          if (typeof drawApi.getMode !== "function" || drawApi.getMode() !== "draw_polygon") {
            src.setData(EMPTY_FC as any);
            return;
          }
          const all = draw.getAll();
          const f = all?.features?.[0];
          if (!f || f.geometry?.type !== "Polygon") {
            src.setData(EMPTY_FC as any);
            return;
          }
          const ring = f.geometry.coordinates?.[0];
          if (!ring || ring.length < 2) {
            src.setData(EMPTY_FC as any);
            return;
          }
          // MapboxDraw Polygon.toGeoJSON closes the ring: two in-progress verts [A,B] become [A,B,A].
          const closedAsFirstEdge =
            ring.length === 3 &&
            ring[0]?.length >= 2 &&
            ring[2]?.length >= 2 &&
            ring[0][0] === ring[2][0] &&
            ring[0][1] === ring[2][1];
          const openTwoVerts = ring.length === 2;
          if (!closedAsFirstEdge && !openTwoVerts) {
            src.setData(EMPTY_FC as any);
            return;
          }
          const a = ring[0];
          const b = ring[1];
          if (!a || !b || a.length < 2 || b.length < 2) {
            src.setData(EMPTY_FC as any);
            return;
          }
          src.setData({
            type: "FeatureCollection",
            features: [
              {
                type: "Feature",
                properties: {},
                geometry: {
                  type: "LineString",
                  coordinates: [
                    [a[0], a[1]],
                    [b[0], b[1]],
                  ],
                },
              },
            ],
          } as any);
        };

        map.on("draw.render", syncPolygonPreviewLine);
        map.on("mousemove", syncPolygonPreviewLine);
        map.on("draw.modechange", syncPolygonPreviewLine);
        map.on("draw.modechange", notifyDrawMode);
        notifyDrawMode();

        map.on("draw.create", (e: { features?: GeoJSON.Feature[] }) => {
          let feature = e?.features?.[0];
          if (!feature && typeof draw.getAll === "function") {
            const all = draw.getAll();
            feature = all?.features?.[0];
          }
          if (feature?.geometry) {
            const geom = normalizeBlockageGeometry(feature.geometry);
            onBlockageChange(geom);
          }
          syncPolygonPreviewLine();
        });
        map.on("draw.update", (e: { features?: GeoJSON.Feature[] }) => {
          let feature = e?.features?.[0];
          if (!feature && typeof draw.getAll === "function") {
            const all = draw.getAll();
            feature = all?.features?.[0];
          }
          if (feature?.geometry) {
            const geom = normalizeBlockageGeometry(feature.geometry);
            onBlockageChange(geom);
          }
          syncPolygonPreviewLine();
        });
        map.on("draw.delete", () => {
          onBlockageChange(null);
          syncPolygonPreviewLine();
        });
        drawDetachRef.current = () => {
          map.off("draw.render", syncPolygonPreviewLine);
          map.off("mousemove", syncPolygonPreviewLine);
          map.off("draw.modechange", syncPolygonPreviewLine);
          map.off("draw.modechange", notifyDrawMode);
        };
      }

      if (!map.getSource(SOURCE_POLYGON_PREVIEW)) {
        map.addSource(SOURCE_POLYGON_PREVIEW, { type: "geojson", data: EMPTY_FC });
      }
      if (!map.getLayer(LAYER_POLYGON_PREVIEW)) {
        // Must match MapboxDraw's suffixed layer id (see @mapbox/mapbox-gl-draw options.js addSources).
        if (map.getLayer(DRAW_VERTEX_LAYER_HOT)) {
          map.addLayer(
            {
              id: LAYER_POLYGON_PREVIEW,
              type: "line",
              source: SOURCE_POLYGON_PREVIEW,
              layout: { "line-cap": "round", "line-join": "round" },
              paint: { "line-color": "#dc2626", "line-width": 2 },
            },
            DRAW_VERTEX_LAYER_HOT
          );
        } else {
          map.addLayer({
            id: LAYER_POLYGON_PREVIEW,
            type: "line",
            source: SOURCE_POLYGON_PREVIEW,
            layout: { "line-cap": "round", "line-join": "round" },
            paint: { "line-color": "#dc2626", "line-width": 2 },
          });
        }
      }
    };

    const rehydrateMapStyle = () => {
      ensureCoreSourcesAndLayers();
      const routeSource = map.getSource(SOURCE_ROUTE) as maplibregl.GeoJSONSource | undefined;
      routeSource?.setData((routeRef.current || EMPTY_FC) as any);
      const detourSource = map.getSource(SOURCE_DETOUR) as maplibregl.GeoJSONSource | undefined;
      detourSource?.setData((detourRef.current?.path_geojson || EMPTY_FC) as any);
      const pinSource = map.getSource(SOURCE_PIN) as maplibregl.GeoJSONSource | undefined;
      if (pinSource) {
        const activePin = pinPositionRef.current;
        pinSource.setData(
          activePin
            ? ({
                type: "FeatureCollection",
                features: [
                  {
                    type: "Feature",
                    geometry: { type: "Point", coordinates: [activePin[1], activePin[0]] },
                    properties: {},
                  },
                ],
              } as any)
            : (EMPTY_FC as any)
        );
      }
      const stopSource = map.getSource(SOURCE_STOPS) as maplibregl.GeoJSONSource | undefined;
      if (stopSource) {
        const activeStops = stopsRef.current;
        const roleByStopId = rolesForStops(activeStops);
        const activeSelectedStopId = selectedStopIdRef.current;
        stopSource.setData({
          type: "FeatureCollection",
          features: activeStops.map((s) => ({
            type: "Feature" as const,
            geometry: { type: "Point" as const, coordinates: [s.lon, s.lat] },
            properties: {
              stop_id: s.stop_id,
              stop_name: s.name,
              stop_code: s.stop_code ?? null,
              is_selected: !!activeSelectedStopId && s.stop_id === activeSelectedStopId,
              role: roleByStopId.get(s.stop_id) ?? "middle",
            },
          })),
        } as any);
      }
      const drawAfterRehydrate = drawRef.current as MapboxDraw & { getMode?: () => string };
      if (drawAfterRehydrate && typeof drawAfterRehydrate.getMode === "function") {
        onDrawModeChangeRef.current?.(drawAfterRehydrate.getMode());
      }
      setMapReady(true);
      requestAnimationFrame(scheduleResize);
    };
    styleRehydrateRef.current = rehydrateMapStyle;

    const onLoad = () => {
      rehydrateMapStyle();
    };

    map.on("load", onLoad);
    return () => {
      styleRehydrateRef.current = null;
      stopLayerClickRef.current && map.off("click", "stops-circles", stopLayerClickRef.current);
      stopLayerMouseEnterRef.current && map.off("mouseenter", "stops-circles", stopLayerMouseEnterRef.current);
      stopLayerMouseLeaveRef.current && map.off("mouseleave", "stops-circles", stopLayerMouseLeaveRef.current);
      stopLayerClickRef.current = null;
      stopLayerMouseEnterRef.current = null;
      stopLayerMouseLeaveRef.current = null;
      drawDetachRef.current?.();
      drawDetachRef.current = null;
      closeStopPopup();
      resizeObserver?.disconnect();
      window.removeEventListener("resize", scheduleResize);
      map.off("load", onLoad);
      map.remove();
      mapRef.current = null;
      drawRef.current = null;
      setMapReady(false);
    };
  }, [onBlockageChange]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const prev = basemapRef.current;
    if (prev === basemap) return;

    const camera = {
      center: map.getCenter(),
      zoom: map.getZoom(),
      bearing: map.getBearing(),
      pitch: map.getPitch(),
    };

    const applyCamera = () => {
      map.jumpTo(camera);
    };

    // Raster->raster: keep style/sources/layers and only swap tile URLs.
    if (isRasterBasemap(prev) && isRasterBasemap(basemap)) {
      const src = map.getSource(BASEMAP_RASTER_SOURCE) as (maplibregl.Source & { setTiles?: (t: string[]) => void }) | null;
      src?.setTiles?.(rasterTilesForBasemap(basemap));
      basemapRef.current = basemap;
      applyCamera();
      return;
    }

    setMapReady(false);
    map.once("style.load", () => {
      styleRehydrateRef.current?.();
      applyCamera();
      basemapRef.current = basemap;
    });
    map.setStyle(styleForBasemap(basemap), { diff: false });
  }, [basemap]);

  // After style reload, re-apply blockage from React state into MapboxDraw.
  useEffect(() => {
    if (!mapReady || !drawRef.current) return;
    const draw = drawRef.current;
    const geom = blockagePropRef.current;
    try {
      draw.deleteAll();
      if (geom) {
        draw.add({
          type: "Feature",
          geometry: geom as GeoJSON.Geometry,
          properties: {},
        } as GeoJSON.Feature);
      }
    } catch {
      /* ignore restore errors */
    }
    const drawNotify = drawRef.current as MapboxDraw & { getMode?: () => string };
    if (drawNotify && typeof drawNotify.getMode === "function") {
      onDrawModeChangeRef.current?.(drawNotify.getMode());
    }
  }, [mapReady]);

  // Route source data
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) return;
    const source = map.getSource(SOURCE_ROUTE) as maplibregl.GeoJSONSource | undefined;
    if (!source) return;
    const data = routeGeojson || ({ type: "FeatureCollection", features: [] } as GeoJSON.FeatureCollection);
    source.setData(data as any);
  }, [mapReady, routeGeojson]);

  // Detour source data
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) return;
    const source = map.getSource(SOURCE_DETOUR) as maplibregl.GeoJSONSource | undefined;
    if (!source) return;
    const detourFc = detour?.path_geojson ?? null;
    const data = detourFc || ({ type: "FeatureCollection", features: [] } as GeoJSON.FeatureCollection);
    source.setData(data as any);
  }, [mapReady, detour]);

  // Stops source data
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) return;
    const source = map.getSource(SOURCE_STOPS) as maplibregl.GeoJSONSource | undefined;
    if (!source) return;
    const roleByStopId = rolesForStops(stops);
    const stopsFc: GeoJSONFeatureCollection = {
      type: "FeatureCollection",
      features: stops.map((s) => ({
        type: "Feature" as const,
        geometry: { type: "Point" as const, coordinates: [s.lon, s.lat] },
        properties: {
          stop_id: s.stop_id,
          stop_name: s.name,
          stop_code: s.stop_code ?? null,
          is_selected: !!selectedStopId && s.stop_id === selectedStopId,
          role: roleByStopId.get(s.stop_id) ?? "middle",
        },
      })),
    };
    source.setData(stopsFc as any);
  }, [mapReady, stops, selectedStopId]);

  // Pin source data
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) return;
    const source = map.getSource(SOURCE_PIN) as maplibregl.GeoJSONSource | undefined;
    if (!source) return;
    const pinFc: GeoJSONFeatureCollection = pinPosition
      ? {
          type: "FeatureCollection",
          features: [
            {
              type: "Feature",
              geometry: { type: "Point" as const, coordinates: [pinPosition[1], pinPosition[0]] },
              properties: {},
            },
          ],
        }
      : { type: "FeatureCollection", features: [] };
    source.setData(pinFc as any);
  }, [mapReady, pinPosition]);

  const fitBoundsFromGeoJSON = (geojson: GeoJSON.FeatureCollection | null) => {
    const map = mapRef.current;
    if (!map || !geojson?.features?.length) return;
    const bounds = new maplibregl.LngLatBounds();
    const extend = (coord: number[]) => {
      if (coord.length >= 2) bounds.extend([coord[0], coord[1]]);
    };
    geojson.features.forEach((f) => {
      const g = f.geometry;
      if (g.type === "Point") extend(g.coordinates);
      else if (g.type === "LineString") g.coordinates.forEach(extend);
      else if (g.type === "Polygon") g.coordinates[0].forEach(extend);
    });
    if (bounds.isEmpty()) return;
    map.fitBounds(bounds, { padding: 50, maxZoom: 16 });
  };

  useImperativeHandle(
    ref,
    () => ({
      clearBlockage() {
        drawRef.current?.deleteAll();
        onBlockageChange(null);
      },
      startPolygon() {
        const draw = drawRef.current;
        if (!draw) return;
        try {
          // Switch MapboxDraw into polygon drawing mode explicitly.
          // @ts-expect-error changeMode is available at runtime.
          draw.changeMode("draw_polygon");
        } catch (e) {
          console.error("Failed to start draw_polygon mode", e);
        }
      },
      editBlockagePolygon() {
        const draw = drawRef.current;
        if (!draw) return;
        let all = draw.getAll() as GeoJSON.FeatureCollection;
        if (!all.features?.length && blockagePropRef.current) {
          try {
            draw.deleteAll();
            draw.add({
              type: "Feature",
              geometry: blockagePropRef.current as GeoJSON.Geometry,
              properties: {},
            } as GeoJSON.Feature);
            all = draw.getAll() as GeoJSON.FeatureCollection;
          } catch (e) {
            console.error("Failed to re-seed blockage for edit", e);
            return;
          }
        }
        const featureId = firstPolygonFeatureIdFromDrawAll(all);
        if (!featureId) return;
        try {
          // @ts-expect-error direct_select + featureId at runtime
          draw.changeMode("direct_select", { featureId });
        } catch (e) {
          console.error("Failed to enter direct_select mode", e);
        }
      },
      cancelDrawing() {
        drawRef.current?.changeMode("simple_select");
      },
      undoLastPoint() {
        // MapboxDraw doesn't expose undo easily; no-op or could implement with custom mode
      },
      fitToBlockage() {
        if (!blockageRef.current) return;
        fitBoundsFromGeoJSON({
          type: "FeatureCollection",
          features: [{ type: "Feature", geometry: blockageRef.current, properties: {} }],
        } as GeoJSON.FeatureCollection);
      },
      fitToRoute() {
        fitBoundsFromGeoJSON(routeRef.current ?? null);
      },
      fitToDetour() {
        const d = detourRef.current;
        if (d?.path_geojson) fitBoundsFromGeoJSON(d.path_geojson);
      },
      flyTo(lat: number, lng: number, zoom = 15) {
        mapRef.current?.flyTo({ center: [lng, lat], zoom });
      },
    }),
    [onBlockageChange]
  );

  return <div ref={containerRef} className="maplibre-map-container" />;
});

export default MapLibreMap;

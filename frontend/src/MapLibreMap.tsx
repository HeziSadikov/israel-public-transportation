import "maplibre-gl/dist/maplibre-gl.css";
import "@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css";

import React, { useEffect, useImperativeHandle, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import MapboxDraw from "@mapbox/mapbox-gl-draw";

import {
  govmapAttribution,
  govmapRasterMaxZoom,
  govmapRasterTiles,
  isGovmapBasemapConfigured,
} from "./govmapBasemapEnv";

// GeoJSON types for internal use (coordinates as [lng, lat])
type GeoJSONPosition = [number, number];
type GeoJSONGeometry =
  | { type: "Polygon"; coordinates: GeoJSONPosition[][] }
  | { type: "LineString"; coordinates: GeoJSONPosition[] }
  | { type: "MultiLineString"; coordinates: GeoJSONPosition[][] }
  | { type: "MultiPolygon"; coordinates: GeoJSONPosition[][][] }
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

export type BasemapKind = "osm" | "carto_light" | "vector_liberty" | "govmap";
export type AllRoutesScope = "all" | "time_window";
export type AllRoutesRenderMode = "always_visible" | "balanced";
const BASEMAP_RASTER_SOURCE = "basemap-raster-source";
const BASEMAP_RASTER_LAYER = "basemap-raster";

/** OpenFreeMap Liberty — vector tiles, crisp at all zooms (separate CDN; check terms for production). */
const OPENFREEMAP_LIBERTY_STYLE = "https://tiles.openfreemap.org/styles/liberty";

function effectiveBasemap(kind: BasemapKind): BasemapKind {
  if (kind !== "govmap") return kind;
  if (!isGovmapBasemapConfigured()) return "osm";
  if (govmapRasterTiles().length === 0) return "osm";
  return "govmap";
}

function rasterStyle(tiles: string[], attribution: string, maxzoom = 19): maplibregl.StyleSpecification {
  return {
    version: 8,
    sources: {
      [BASEMAP_RASTER_SOURCE]: {
        type: "raster",
        tiles,
        tileSize: 256,
        attribution,
        maxzoom,
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
  if (kind === "govmap") {
    return rasterStyle(govmapRasterTiles(), govmapAttribution(), govmapRasterMaxZoom());
  }
  return OSM_STYLE;
}

function isRasterBasemap(kind: BasemapKind): boolean {
  return kind === "osm" || kind === "carto_light" || kind === "govmap";
}

function rasterMaxZoom(kind: BasemapKind): number {
  if (kind === "govmap") return govmapRasterMaxZoom();
  return 19;
}

function mapMaxZoomForBasemap(kind: BasemapKind): number {
  if (kind === "vector_liberty") return 19;
  return rasterMaxZoom(kind);
}

function rasterTilesForBasemap(kind: BasemapKind): string[] {
  if (kind === "carto_light") {
    return [
      "https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
      "https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
      "https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
    ];
  }
  if (kind === "govmap") return govmapRasterTiles();
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

const INTEGER_ZOOM_EPSILON = 0.01;

/** MapboxDraw styles filter on this; polygon-phase synthetic LineStrings omit it (see mapbox-gl-draw draw_polygon). */
const PROP_MANUAL_DETOUR_DRAFT = "manual_detour_draft";
const VAL_MANUAL_DETOUR_DRAFT = "yes";

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
  // Untagged LineStrings (polygon first-edge preview + in-progress draw_line_string): do NOT filter on `active`.
  // MapboxDraw often omits `active` on HOT LineStrings while drawing, which made line layers match nothing (only vertices visible).
  {
    id: "gl-draw-line-untagged",
    type: "line",
    filter: [
      "all",
      ["==", "$type", "LineString"],
      ["!=", ["get", PROP_MANUAL_DETOUR_DRAFT], VAL_MANUAL_DETOUR_DRAFT],
    ],
    layout: { "line-cap": "round", "line-join": "round" },
    paint: {
      "line-color": "#dc2626",
      "line-width": 2,
    },
  },
];

const SOURCE_ROUTE = "route";
const SOURCE_ALL_ROUTES_COVERAGE = "all-routes-coverage";
const SOURCE_ALL_ROUTES = "all-routes";
const LAYER_ALL_ROUTES_COVERAGE = "all-routes-coverage-line";
const LAYER_ALL_ROUTES = "all-routes-line";
const LAYER_ALL_ROUTES_HEAT = "all-routes-heat-line";
const SOURCE_DETOUR = "detour";
const SOURCE_STOPS = "stops";
const SOURCE_PIN = "pin";
/** First-edge rubber line while drawing (one anchor + cursor); not part of MapboxDraw HOT/COLD. */
const SOURCE_POLYGON_PREVIEW = "blockage-polygon-preview-line";
const LAYER_POLYGON_PREVIEW = "blockage-polygon-preview-line-layer";
/** Manual detour draft path — plain GeoJSON overlay (MapboxDraw stroke filters are unreliable for this line). */
const SOURCE_MANUAL_DETOUR_DRAFT = "manual-detour-draft";
const LAYER_MANUAL_DETOUR_DRAFT = "manual-detour-draft-line";

const EMPTY_FC: GeoJSONFeatureCollection = { type: "FeatureCollection", features: [] };

function normalizeApiBase(apiBase: string): string {
  return apiBase.endsWith("/") ? apiBase.slice(0, -1) : apiBase;
}

function allRoutesTilesUrl(
  apiBase: string,
  scope: AllRoutesScope,
  renderMode: AllRoutesRenderMode,
  startDate: string,
  startTime: string,
  endDate: string,
  endTime: string
): string {
  const params = new URLSearchParams();
  params.set("scope", scope);
  params.set("render_mode", renderMode);
  if (scope === "time_window") {
    params.set("start_date", startDate);
    params.set("start_time", startTime);
    params.set("end_date", endDate);
    params.set("end_time", endTime);
  }
  return `${normalizeApiBase(apiBase)}/routes/tiles/{z}/{x}/{y}.mvt?${params.toString()}`;
}

function allRoutesCoverageTilesUrl(apiBase: string): string {
  return `${normalizeApiBase(apiBase)}/routes/coverage/{z}/{x}/{y}.mvt`;
}

function baseLineWidthExpr(mode: AllRoutesRenderMode): any[] {
  if (mode === "always_visible") {
    return ["interpolate", ["linear"], ["zoom"], 0, 0.36, 4, 0.52, 7, 0.82, 11, 1.55, 14, 2.4, 16, 3.0, 19, 3.9];
  }
  return ["interpolate", ["linear"], ["zoom"], 0, 0.2, 4, 0.28, 7, 0.42, 11, 1.0, 14, 1.7, 16, 2.2, 19, 2.8];
}

function baseLineOpacityExpr(mode: AllRoutesRenderMode): any[] {
  if (mode === "always_visible") {
    return ["interpolate", ["linear"], ["zoom"], 0, 0.2, 5, 0.25, 8, 0.3, 12, 0.37, 16, 0.43, 19, 0.5];
  }
  return ["interpolate", ["linear"], ["zoom"], 0, 0.12, 5, 0.15, 8, 0.2, 12, 0.27, 16, 0.33, 19, 0.38];
}

function heatLineWidthExpr(mode: AllRoutesRenderMode): any[] {
  if (mode === "always_visible") {
    return ["interpolate", ["linear"], ["zoom"], 0, 0.48, 4, 0.66, 7, 1.0, 11, 2.0, 14, 3.1, 16, 4.0, 19, 5.0];
  }
  return ["interpolate", ["linear"], ["zoom"], 0, 0.28, 4, 0.38, 7, 0.6, 11, 1.5, 14, 2.5, 16, 3.1, 19, 3.8];
}

function heatLineOpacityExpr(mode: AllRoutesRenderMode): any[] {
  if (mode === "always_visible") {
    return ["interpolate", ["linear"], ["zoom"], 0, 0.32, 5, 0.4, 8, 0.5, 12, 0.6, 16, 0.68, 19, 0.74];
  }
  return ["interpolate", ["linear"], ["zoom"], 0, 0.22, 5, 0.3, 8, 0.4, 12, 0.5, 16, 0.57, 19, 0.63];
}

function normalizeToFeatureCollection(input: unknown): GeoJSONFeatureCollection {
  if (!input || typeof input !== "object") return EMPTY_FC;
  const maybe = input as { type?: string; features?: unknown; geometry?: unknown; properties?: unknown };
  if (maybe.type === "FeatureCollection" && Array.isArray(maybe.features)) {
    return maybe as GeoJSONFeatureCollection;
  }
  if (maybe.type === "Feature" && maybe.geometry && typeof maybe.geometry === "object") {
    return {
      type: "FeatureCollection",
      features: [{ type: "Feature", geometry: maybe.geometry as GeoJSONGeometry, properties: maybe.properties as any }],
    };
  }
  if (
    maybe.type === "LineString" ||
    maybe.type === "MultiLineString" ||
    maybe.type === "Polygon" ||
    maybe.type === "MultiPolygon" ||
    maybe.type === "Point"
  ) {
    return {
      type: "FeatureCollection",
      features: [{ type: "Feature", geometry: maybe as GeoJSONGeometry, properties: {} }],
    };
  }
  return EMPTY_FC;
}

function manualDetourDraftOverlayFc(line: GeoJSON.LineString | null): GeoJSONFeatureCollection {
  if (!line?.coordinates || line.coordinates.length < 2) return EMPTY_FC;
  return {
    type: "FeatureCollection",
    features: [{ type: "Feature", properties: {}, geometry: line }],
  };
}

function syncManualDetourDraftOverlay(map: maplibregl.Map, line: GeoJSON.LineString | null) {
  const src = map.getSource(SOURCE_MANUAL_DETOUR_DRAFT) as maplibregl.GeoJSONSource | undefined;
  src?.setData(manualDetourDraftOverlayFc(line) as any);
}

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

function deletePolygonFeaturesFromDraw(draw: MapboxDraw) {
  const all = draw.getAll() as GeoJSON.FeatureCollection;
  for (const f of all.features || []) {
    const t = f.geometry?.type;
    if (t === "Polygon" || t === "MultiPolygon") {
      if (f.id != null) draw.delete(String(f.id));
    }
  }
}

function deleteLineStringFeaturesFromDraw(draw: MapboxDraw) {
  const all = draw.getAll() as GeoJSON.FeatureCollection;
  for (const f of all.features || []) {
    if (f.geometry?.type === "LineString" && f.id != null) {
      draw.delete(String(f.id));
    }
  }
}

function firstLineStringGeometryFromDrawAll(all: GeoJSON.FeatureCollection): GeoJSON.LineString | null {
  const f = all.features?.find((x) => x.geometry?.type === "LineString");
  const g = f?.geometry;
  return g && g.type === "LineString" ? g : null;
}

export type MapLibreMapHandle = {
  clearBlockage: () => void;
  cancelDrawing: () => void;
  undoLastPoint: () => void;
  startPolygon: () => void;
  editBlockagePolygon: () => void;
  /** Replace MapboxDraw blockage polygon and sync parent state (e.g. after pasting coordinate text). */
  applyBlockagePolygonToDraw: (geom: GeoJSON.Geometry | null) => void;
  /** Replace the drawn manual detour LineString (e.g. after pasting GeoJSON in the panel). */
  applyManualDetourLineToDraw: (geom: GeoJSON.LineString | null) => void;
  startDrawDetourLine: () => void;
  clearManualDetourLine: () => void;
  fitToBlockage: () => void;
  fitToRoute: () => void;
  fitToDetour: () => void;
  flyTo: (lat: number, lng: number, zoom?: number) => void;
};

export type MapLibreMapProps = {
  apiBase: string;
  /** Center as [lat, lng] (converted to [lng, lat] for MapLibre) */
  center: [number, number];
  stops: { stop_id: string; name: string; stop_code?: string | null; lat: number; lon: number; sequence: number }[];
  routeGeojson: GeoJSON.FeatureCollection | null;
  detour: { path_geojson?: unknown } | null;
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
  /** Basemap: OSM / Carto / GovMap raster (EPSG:3857 tiles), or vector (OpenFreeMap Liberty). */
  basemap?: BasemapKind;
  /** Fired when MapboxDraw mode changes (e.g. draw_polygon, direct_select, simple_select). */
  onDrawModeChange?: (mode: string) => void;
  /** Draft detour path drawn on the map (LineString lon/lat), separate from computed detour overlay. */
  manualDraftDetourLine?: GeoJSON.LineString | null;
  /** Called when the user finishes or edits the manual detour line in MapboxDraw. */
  onManualDetourLineChange?: (geom: GeoJSON.LineString | null) => void;
  /** Show country-level routes layer sourced from vector tiles. */
  showAllRoutesLayer?: boolean;
  /** Layer scope: all feed routes or routes active in selected time window. */
  allRoutesScope?: AllRoutesScope;
  /** Time window passed to vector-tile API when allRoutesScope is time_window. */
  allRoutesStartDate?: string;
  allRoutesStartTime?: string;
  allRoutesEndDate?: string;
  allRoutesEndTime?: string;
  showRoutesHeatLayer?: boolean;
  allRoutesRenderMode?: AllRoutesRenderMode;
  /** Detour v2 overlay: exit/rejoin anchor pins and skipped-stop markers (E4). */
  detourV2Overlay?: {
    exitLon?: number | null;
    exitLat?: number | null;
    rejoinLon?: number | null;
    rejoinLat?: number | null;
    exitStopId?: string | null;
    rejoinStopId?: string | null;
    skippedStopIds?: string[];
  } | null;
  /** Optional debug lines from detour v2 `debug.geojson` (merged into same overlay source). */
  detourV2DebugGeojson?: GeoJSON.FeatureCollection | null;
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
    apiBase,
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
    manualDraftDetourLine = null,
    onManualDetourLineChange,
    showAllRoutesLayer = false,
    detourV2Overlay = null,
    detourV2DebugGeojson = null,
    allRoutesScope = "all",
    allRoutesStartDate = "",
    allRoutesStartTime = "",
    allRoutesEndDate = "",
    allRoutesEndTime = "",
    showRoutesHeatLayer = false,
    allRoutesRenderMode = "balanced",
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
  const basemapRef = useRef<BasemapKind>(effectiveBasemap(basemap));
  const initialBasemapRef = useRef<BasemapKind>(effectiveBasemap(basemap));
  const [mapReady, setMapReady] = useState(false);
  const allRoutesTileUrlRef = useRef("");
  const allRoutesTileUrlAppliedRef = useRef("");
  const allRoutesCoverageTileUrlRef = useRef("");
  const allRoutesCoverageTileUrlAppliedRef = useRef("");
  const showAllRoutesLayerRef = useRef(false);
  const showRoutesHeatLayerRef = useRef(false);
  const allRoutesRenderModeRef = useRef<AllRoutesRenderMode>("balanced");

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
  const onBlockageChangeRef = useRef(onBlockageChange);
  onBlockageChangeRef.current = onBlockageChange;
  const onManualDetourLineChangeRef = useRef<MapLibreMapProps["onManualDetourLineChange"]>(undefined);
  onManualDetourLineChangeRef.current = onManualDetourLineChange;
  const manualDraftDetourLinePropRef = useRef<GeoJSON.LineString | null>(null);
  manualDraftDetourLinePropRef.current = manualDraftDetourLine;
  allRoutesTileUrlRef.current = allRoutesTilesUrl(
    apiBase,
    allRoutesScope,
    allRoutesRenderMode,
    allRoutesStartDate,
    allRoutesStartTime,
    allRoutesEndDate,
    allRoutesEndTime
  );
  allRoutesCoverageTileUrlRef.current = allRoutesCoverageTilesUrl(apiBase);
  showAllRoutesLayerRef.current = showAllRoutesLayer;
  showRoutesHeatLayerRef.current = showRoutesHeatLayer;
  allRoutesRenderModeRef.current = allRoutesRenderMode;

  useEffect(() => {
    if (!containerRef.current) return;
    const [lat, lng] = center;
    const eff0 = effectiveBasemap(basemap);
    initialBasemapRef.current = eff0;
    basemapRef.current = eff0;
    const mapOptions = {
      container: containerRef.current,
      style: styleForBasemap(eff0),
      center: [lng, lat],
      zoom: 9,
      minZoom: 5,
      maxZoom: mapMaxZoomForBasemap(eff0),
      dragRotate: false,
      pitchWithRotate: false,
      pixelRatio: mapPixelRatioForBasemap(eff0),
      // `zoomSnap` is available in newer MapLibre releases; unknown options are ignored on older builds.
      zoomSnap: 1,
    } as maplibregl.MapOptions & { zoomSnap?: number };
    const map = new maplibregl.Map(mapOptions);
    // Make mouse-wheel zoom a bit faster/more responsive.
    map.scrollZoom.setWheelZoomRate(1 / 180);
    map.scrollZoom.setZoomRate(1.0);
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    mapRef.current = map;
    (window as Window & { __osmComparisonSnapshot?: () => Record<string, unknown> }).__osmComparisonSnapshot = () => {
      const centerNow = map.getCenter();
      const intZoom = Math.round(map.getZoom());
      return {
        center: { lat: Number(centerNow.lat.toFixed(6)), lng: Number(centerNow.lng.toFixed(6)) },
        mapZoom: Number(map.getZoom().toFixed(3)),
        integerZoom: intZoom,
        viewport: { width: window.innerWidth, height: window.innerHeight },
        devicePixelRatio: window.devicePixelRatio || 1,
        openStreetMapUrl: `https://www.openstreetmap.org/#map=${intZoom}/${centerNow.lat.toFixed(6)}/${centerNow.lng.toFixed(6)}`,
      };
    };

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
    let pendingIntegerSnap = false;
    let lastWheelAt = 0;
    const markPendingIntegerSnap = () => {
      pendingIntegerSnap = true;
    };
    const onMapWheel = () => {
      lastWheelAt = Date.now();
      pendingIntegerSnap = false;
    };
    const onMapZoomEnd = () => {
      if (!pendingIntegerSnap) return;
      pendingIntegerSnap = false;
      if (Date.now() - lastWheelAt < 150) return;
      const currentZoom = map.getZoom();
      const targetZoom = Math.round(currentZoom);
      if (Math.abs(currentZoom - targetZoom) <= INTEGER_ZOOM_EPSILON) return;
      map.easeTo({ zoom: targetZoom, duration: 120 });
    };
    const onWindowKeyDown = (event: KeyboardEvent) => {
      if (!containerEl || !document.activeElement || !containerEl.contains(document.activeElement)) return;
      const isZoomKey =
        event.key === "+" ||
        event.key === "=" ||
        event.key === "-" ||
        event.key === "_" ||
        event.key === "NumpadAdd" ||
        event.key === "NumpadSubtract";
      if (isZoomKey) markPendingIntegerSnap();
    };
    map.on("dblclick", markPendingIntegerSnap);
    map.on("wheel", onMapWheel);
    map.on("zoomend", onMapZoomEnd);
    window.addEventListener("keydown", onWindowKeyDown, true);
    const navControlButtons = containerEl?.querySelectorAll<HTMLButtonElement>(
      ".maplibregl-ctrl-group button.maplibregl-ctrl-zoom-in, .maplibregl-ctrl-group button.maplibregl-ctrl-zoom-out"
    );
    navControlButtons?.forEach((btn) => btn.addEventListener("click", markPendingIntegerSnap, true));
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

      if (!map.getSource(SOURCE_ALL_ROUTES)) {
        map.addSource(SOURCE_ALL_ROUTES, {
          type: "vector",
          tiles: [allRoutesTileUrlRef.current],
          minzoom: 0,
          maxzoom: 22,
        } as maplibregl.VectorTileSourceSpecification);
        allRoutesTileUrlAppliedRef.current = allRoutesTileUrlRef.current;
      }
      if (!map.getSource(SOURCE_ALL_ROUTES_COVERAGE)) {
        map.addSource(SOURCE_ALL_ROUTES_COVERAGE, {
          type: "vector",
          tiles: [allRoutesCoverageTileUrlRef.current],
          minzoom: 0,
          maxzoom: 22,
        } as maplibregl.VectorTileSourceSpecification);
        allRoutesCoverageTileUrlAppliedRef.current = allRoutesCoverageTileUrlRef.current;
      }
      if (!map.getLayer(LAYER_ALL_ROUTES_COVERAGE)) {
        map.addLayer(
          {
            id: LAYER_ALL_ROUTES_COVERAGE,
            type: "line",
            source: SOURCE_ALL_ROUTES_COVERAGE,
            "source-layer": "coverage",
            minzoom: 0,
            maxzoom: 10.5,
            layout: {
              visibility: showAllRoutesLayerRef.current ? "visible" : "none",
              "line-cap": "round",
              "line-join": "round",
            },
            paint: {
              "line-color": "#2563eb",
              "line-width": ["interpolate", ["linear"], ["zoom"], 0, 0.6, 3, 0.8, 6, 1.1, 9, 1.6, 10.5, 2.0],
              "line-opacity": ["interpolate", ["linear"], ["zoom"], 0, 0.3, 4, 0.35, 8, 0.42, 10.5, 0.48],
            },
          },
          "route-line"
        );
      } else {
        map.setLayoutProperty(
          LAYER_ALL_ROUTES_COVERAGE,
          "visibility",
          showAllRoutesLayerRef.current ? "visible" : "none"
        );
      }
      if (!map.getLayer(LAYER_ALL_ROUTES)) {
        map.addLayer(
          {
            id: LAYER_ALL_ROUTES,
            type: "line",
            source: SOURCE_ALL_ROUTES,
            "source-layer": "routes",
            minzoom: 7,
            layout: {
              visibility: showAllRoutesLayerRef.current ? "visible" : "none",
              "line-cap": "round",
              "line-join": "round",
            },
            paint: {
              "line-color": "#1d4ed8",
              "line-width": baseLineWidthExpr(allRoutesRenderModeRef.current),
              "line-opacity": baseLineOpacityExpr(allRoutesRenderModeRef.current),
            },
          },
          "route-line"
        );
      } else {
        map.setLayoutProperty(
          LAYER_ALL_ROUTES,
          "visibility",
          showAllRoutesLayerRef.current ? "visible" : "none"
        );
      }
      if (!map.getLayer(LAYER_ALL_ROUTES_HEAT)) {
        map.addLayer(
          {
            id: LAYER_ALL_ROUTES_HEAT,
            type: "line",
            source: SOURCE_ALL_ROUTES,
            "source-layer": "routes",
            minzoom: 7,
            layout: {
              visibility:
                showAllRoutesLayerRef.current && showRoutesHeatLayerRef.current ? "visible" : "none",
              "line-cap": "round",
              "line-join": "round",
            },
            paint: {
              "line-color": [
                "interpolate",
                ["linear"],
                ["coalesce", ["get", "intensity"], 0],
                0,
                "#1d4ed8",
                0.35,
                "#06b6d4",
                0.6,
                "#f59e0b",
                1,
                "#dc2626",
              ],
              "line-width": heatLineWidthExpr(allRoutesRenderModeRef.current),
              "line-opacity": heatLineOpacityExpr(allRoutesRenderModeRef.current),
            },
          },
          "route-line"
        );
      } else {
        map.setLayoutProperty(
          LAYER_ALL_ROUTES_HEAT,
          "visibility",
          showAllRoutesLayerRef.current && showRoutesHeatLayerRef.current ? "visible" : "none"
        );
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

      // Detour v2 anchor + skipped-stop overlay (E4).
      const SOURCE_V2_OVERLAY = "detour-v2-overlay";
      if (!map.getSource(SOURCE_V2_OVERLAY)) {
        map.addSource(SOURCE_V2_OVERLAY, { type: "geojson", data: emptyFc });
        map.addLayer({
          id: "detour-v2-skipped-stops",
          type: "circle",
          source: SOURCE_V2_OVERLAY,
          filter: ["==", ["get", "role"], "skipped"],
          paint: {
            "circle-radius": 7,
            "circle-color": "#9ca3af",
            "circle-stroke-color": "#6b7280",
            "circle-stroke-width": 1.5,
          },
        });
        map.addLayer({
          id: "detour-v2-anchor-exit",
          type: "circle",
          source: SOURCE_V2_OVERLAY,
          filter: ["==", ["get", "role"], "exit"],
          paint: {
            "circle-radius": 9,
            "circle-color": "#f59e0b",
            "circle-stroke-color": "#92400e",
            "circle-stroke-width": 2,
          },
        });
        map.addLayer({
          id: "detour-v2-anchor-rejoin",
          type: "circle",
          source: SOURCE_V2_OVERLAY,
          filter: ["==", ["get", "role"], "rejoin"],
          paint: {
            "circle-radius": 9,
            "circle-color": "#3b82f6",
            "circle-stroke-color": "#1e40af",
            "circle-stroke-width": 2,
          },
        });
        map.addLayer({
          id: "detour-v2-debug-lines",
          type: "line",
          source: SOURCE_V2_OVERLAY,
          filter: ["all", ["has", "layer"], ["==", ["geometry-type"], "LineString"]],
          paint: {
            "line-color": [
              "match",
              ["get", "layer"],
              "matched_physical",
              "#a855f7",
              "gtfs_shape",
              "#64748b",
              "blocked_span_matched",
              "#f97316",
              "raw_valhalla_detour",
              "#0ea5e9",
              "decoded_detour",
              "#22c55e",
              "#94a3b8",
            ],
            "line-width": 3,
            "line-opacity": 0.85,
          },
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
        // Draw control: polygon (blockage) + line_string (manual detour path); activated from toolbar / Advanced.
        const draw = new MapboxDraw({
          displayControlsDefault: false,
          controls: {
            polygon: true,
            line_string: true,
            trash: true,
          },
          styles: DRAW_STYLES,
        });
        // Place controls in the top-right to avoid overlap with the explorer window.
        map.addControl(draw as any, "top-right");
        drawRef.current = draw;

        const publishDrawState = () => {
          const d = drawRef.current;
          if (!d) return;
          const all = d.getAll() as GeoJSON.FeatureCollection;
          const poly = all.features?.find(
            (f) => f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon"
          );
          if (poly?.geometry) {
            onBlockageChangeRef.current?.(normalizeBlockageGeometry(poly.geometry as GeoJSON.Geometry));
          } else {
            onBlockageChangeRef.current?.(null);
          }
          const line = firstLineStringGeometryFromDrawAll(all);
          onManualDetourLineChangeRef.current?.(line);
          syncManualDetourDraftOverlay(map, line);
        };

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
          const f = all?.features?.find((x) => x.geometry?.type === "Polygon");
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

        const onDrawRender = () => {
          syncPolygonPreviewLine();
          const d = drawRef.current;
          if (!d) return;
          const line = firstLineStringGeometryFromDrawAll(d.getAll() as GeoJSON.FeatureCollection);
          syncManualDetourDraftOverlay(map, line);
        };
        map.on("draw.render", onDrawRender);
        map.on("mousemove", syncPolygonPreviewLine);
        map.on("draw.modechange", syncPolygonPreviewLine);
        map.on("draw.modechange", notifyDrawMode);
        notifyDrawMode();

        map.on("draw.create", () => {
          publishDrawState();
          syncPolygonPreviewLine();
        });
        map.on("draw.update", () => {
          publishDrawState();
          syncPolygonPreviewLine();
        });
        map.on("draw.delete", () => {
          publishDrawState();
          syncPolygonPreviewLine();
        });
        drawDetachRef.current = () => {
          map.off("draw.render", onDrawRender);
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

      if (!map.getSource(SOURCE_MANUAL_DETOUR_DRAFT)) {
        map.addSource(SOURCE_MANUAL_DETOUR_DRAFT, { type: "geojson", data: EMPTY_FC });
      }
      if (!map.getLayer(LAYER_MANUAL_DETOUR_DRAFT)) {
        map.addLayer({
          id: LAYER_MANUAL_DETOUR_DRAFT,
          type: "line",
          source: SOURCE_MANUAL_DETOUR_DRAFT,
          layout: { "line-cap": "round", "line-join": "round" },
          paint: { "line-color": "#b45309", "line-width": 5 },
        });
      }
    };

    const rehydrateMapStyle = () => {
      ensureCoreSourcesAndLayers();
      const routeSource = map.getSource(SOURCE_ROUTE) as maplibregl.GeoJSONSource | undefined;
      routeSource?.setData((routeRef.current || EMPTY_FC) as any);
      const detourSource = map.getSource(SOURCE_DETOUR) as maplibregl.GeoJSONSource | undefined;
      detourSource?.setData(normalizeToFeatureCollection(detourRef.current?.path_geojson) as any);
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
      syncManualDetourDraftOverlay(map, manualDraftDetourLinePropRef.current);
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
      window.removeEventListener("keydown", onWindowKeyDown, true);
      map.off("dblclick", markPendingIntegerSnap);
      map.off("wheel", onMapWheel);
      map.off("zoomend", onMapZoomEnd);
      navControlButtons?.forEach((btn) => btn.removeEventListener("click", markPendingIntegerSnap, true));
      const win = window as Window & { __osmComparisonSnapshot?: () => Record<string, unknown> };
      delete win.__osmComparisonSnapshot;
      map.off("load", onLoad);
      map.remove();
      mapRef.current = null;
      drawRef.current = null;
      setMapReady(false);
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const prev = basemapRef.current;
    const next = effectiveBasemap(basemap);
    if (prev === next) return;

    const camera = {
      center: map.getCenter(),
      zoom: map.getZoom(),
      bearing: map.getBearing(),
      pitch: map.getPitch(),
    };

    const applyCamera = () => {
      map.jumpTo(camera);
    };

    // Raster->raster: keep style/sources/layers and only swap tile URLs when maxzoom matches.
    if (isRasterBasemap(prev) && isRasterBasemap(next) && rasterMaxZoom(prev) === rasterMaxZoom(next)) {
      const src = map.getSource(BASEMAP_RASTER_SOURCE) as (maplibregl.Source & { setTiles?: (t: string[]) => void }) | null;
      src?.setTiles?.(rasterTilesForBasemap(next));
      basemapRef.current = next;
      map.setMaxZoom(mapMaxZoomForBasemap(next));
      applyCamera();
      return;
    }

    setMapReady(false);
    map.once("style.load", () => {
      styleRehydrateRef.current?.();
      applyCamera();
      basemapRef.current = next;
      map.setMaxZoom(mapMaxZoomForBasemap(next));
    });
    map.setStyle(styleForBasemap(next), { diff: false });
  }, [basemap]);

  // After map becomes ready (or remounts), re-apply blockage + manual detour line into MapboxDraw without wiping
  // each other. Ongoing edits use draw events; textarea paste uses applyManualDetourLineToDraw on the ref.
  useEffect(() => {
    if (!mapReady || !drawRef.current) return;
    const draw = drawRef.current;
    const geom = blockagePropRef.current;
    const line = manualDraftDetourLinePropRef.current;
    try {
      deletePolygonFeaturesFromDraw(draw);
      deleteLineStringFeaturesFromDraw(draw);
      if (geom) {
        draw.add({
          type: "Feature",
          geometry: geom as GeoJSON.Geometry,
          properties: {},
        } as GeoJSON.Feature);
      }
      if (line?.coordinates && line.coordinates.length >= 2) {
        draw.add({
          type: "Feature",
          geometry: line,
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

  // Manual detour draft overlay (plain GeoJSON; stays in sync when parent sets `manualDraftDetourLine` without Draw events).
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) return;
    syncManualDetourDraftOverlay(map, manualDraftDetourLine);
  }, [mapReady, manualDraftDetourLine]);

  // Route tiles source + all-routes layers (base + heat)
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) return;
    const desiredUrl = allRoutesTileUrlRef.current;
    const currentUrl = allRoutesTileUrlAppliedRef.current;
    if (currentUrl !== desiredUrl) {
      if (map.getLayer(LAYER_ALL_ROUTES_HEAT)) map.removeLayer(LAYER_ALL_ROUTES_HEAT);
      if (map.getLayer(LAYER_ALL_ROUTES)) map.removeLayer(LAYER_ALL_ROUTES);
      if (map.getSource(SOURCE_ALL_ROUTES)) map.removeSource(SOURCE_ALL_ROUTES);
      map.addSource(SOURCE_ALL_ROUTES, {
        type: "vector",
        tiles: [desiredUrl],
        minzoom: 0,
        maxzoom: 22,
      } as maplibregl.VectorTileSourceSpecification);
      map.addLayer(
        {
          id: LAYER_ALL_ROUTES,
          type: "line",
          source: SOURCE_ALL_ROUTES,
          "source-layer": "routes",
          minzoom: 7,
          layout: {
            visibility: showAllRoutesLayer ? "visible" : "none",
            "line-cap": "round",
            "line-join": "round",
          },
          paint: {
            "line-color": "#1d4ed8",
            "line-width": baseLineWidthExpr(allRoutesRenderMode),
            "line-opacity": baseLineOpacityExpr(allRoutesRenderMode),
          },
        },
        "route-line"
      );
      map.addLayer(
        {
          id: LAYER_ALL_ROUTES_HEAT,
          type: "line",
          source: SOURCE_ALL_ROUTES,
          "source-layer": "routes",
          minzoom: 7,
          layout: {
            visibility: showAllRoutesLayer && showRoutesHeatLayer ? "visible" : "none",
            "line-cap": "round",
            "line-join": "round",
          },
          paint: {
            "line-color": [
              "interpolate",
              ["linear"],
              ["coalesce", ["get", "intensity"], 0],
              0,
              "#1d4ed8",
              0.35,
              "#06b6d4",
              0.6,
              "#f59e0b",
              1,
              "#dc2626",
            ],
            "line-width": heatLineWidthExpr(allRoutesRenderMode),
            "line-opacity": heatLineOpacityExpr(allRoutesRenderMode),
          },
        },
        "route-line"
      );
      allRoutesTileUrlAppliedRef.current = desiredUrl;
      return;
    }
    if (map.getLayer(LAYER_ALL_ROUTES)) {
      map.setLayoutProperty(LAYER_ALL_ROUTES, "visibility", showAllRoutesLayer ? "visible" : "none");
    }
    if (map.getLayer(LAYER_ALL_ROUTES_COVERAGE)) {
      map.setLayoutProperty(LAYER_ALL_ROUTES_COVERAGE, "visibility", showAllRoutesLayer ? "visible" : "none");
    }
    if (map.getLayer(LAYER_ALL_ROUTES_HEAT)) {
      map.setLayoutProperty(
        LAYER_ALL_ROUTES_HEAT,
        "visibility",
        showAllRoutesLayer && showRoutesHeatLayer ? "visible" : "none"
      );
    }
  }, [
    mapReady,
    showAllRoutesLayer,
    showRoutesHeatLayer,
    allRoutesRenderMode,
    allRoutesScope,
    allRoutesStartDate,
    allRoutesStartTime,
    allRoutesEndDate,
    allRoutesEndTime,
    apiBase,
  ]);

  // Coverage source URL stays stable but can change with API base.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) return;
    const desiredCoverageUrl = allRoutesCoverageTileUrlRef.current;
    if (allRoutesCoverageTileUrlAppliedRef.current === desiredCoverageUrl) return;
    if (map.getLayer(LAYER_ALL_ROUTES_COVERAGE)) map.removeLayer(LAYER_ALL_ROUTES_COVERAGE);
    if (map.getSource(SOURCE_ALL_ROUTES_COVERAGE)) map.removeSource(SOURCE_ALL_ROUTES_COVERAGE);
    map.addSource(SOURCE_ALL_ROUTES_COVERAGE, {
      type: "vector",
      tiles: [desiredCoverageUrl],
      minzoom: 0,
      maxzoom: 22,
    } as maplibregl.VectorTileSourceSpecification);
    map.addLayer(
      {
        id: LAYER_ALL_ROUTES_COVERAGE,
        type: "line",
        source: SOURCE_ALL_ROUTES_COVERAGE,
        "source-layer": "coverage",
        minzoom: 0,
        maxzoom: 10.5,
        layout: {
          visibility: showAllRoutesLayer ? "visible" : "none",
          "line-cap": "round",
          "line-join": "round",
        },
        paint: {
          "line-color": "#2563eb",
          "line-width": ["interpolate", ["linear"], ["zoom"], 0, 0.6, 3, 0.8, 6, 1.1, 9, 1.6, 10.5, 2.0],
          "line-opacity": ["interpolate", ["linear"], ["zoom"], 0, 0.3, 4, 0.35, 8, 0.42, 10.5, 0.48],
        },
      },
      "route-line"
    );
    allRoutesCoverageTileUrlAppliedRef.current = desiredCoverageUrl;
  }, [mapReady, apiBase, showAllRoutesLayer]);

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
    source.setData(normalizeToFeatureCollection(detour?.path_geojson ?? null) as any);
  }, [mapReady, detour]);

  // E4: Update v2 overlay (anchor pins + skipped stops + optional debug lines).
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) return;
    const source = map.getSource("detour-v2-overlay") as maplibregl.GeoJSONSource | undefined;
    if (!source) return;
    const features: GeoJSONFeature[] = [];
    if (detourV2DebugGeojson?.features?.length) {
      for (const f of detourV2DebugGeojson.features) {
        if (f && f.type === "Feature") features.push(f as GeoJSONFeature);
      }
    }
    const ov = detourV2Overlay;
    if (ov) {
      if (ov.exitLon != null && ov.exitLat != null) {
        features.push({
          type: "Feature",
          geometry: { type: "Point", coordinates: [ov.exitLon, ov.exitLat] },
          properties: { role: "exit", stop_id: ov.exitStopId ?? "" },
        });
      }
      if (ov.rejoinLon != null && ov.rejoinLat != null) {
        features.push({
          type: "Feature",
          geometry: { type: "Point", coordinates: [ov.rejoinLon, ov.rejoinLat] },
          properties: { role: "rejoin", stop_id: ov.rejoinStopId ?? "" },
        });
      }
      // Skipped stops: find their lon/lat from the route stops list.
      const skippedSet = new Set(ov.skippedStopIds ?? []);
      for (const stop of stops) {
        if (skippedSet.has(stop.stop_id)) {
          features.push({
            type: "Feature",
            geometry: { type: "Point", coordinates: [stop.lon, stop.lat] },
            properties: { role: "skipped", stop_id: stop.stop_id, name: stop.name },
          });
        }
      }
    }
    source.setData({ type: "FeatureCollection", features } as any);
  }, [mapReady, detourV2Overlay, detourV2DebugGeojson, stops]);

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
      else if (g.type === "MultiLineString") g.coordinates.forEach((line) => line.forEach(extend));
      else if (g.type === "Polygon") g.coordinates[0].forEach(extend);
      else if (g.type === "MultiPolygon") g.coordinates.forEach((poly) => poly[0].forEach(extend));
    });
    if (bounds.isEmpty()) return;
    map.fitBounds(bounds, { padding: 50, maxZoom: 16 });
  };

  useImperativeHandle(
    ref,
    () => ({
      clearBlockage() {
        const draw = drawRef.current;
        if (draw) {
          try {
            deletePolygonFeaturesFromDraw(draw);
          } catch {
            /* ignore */
          }
        }
        onBlockageChangeRef.current?.(null);
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
        const hasPoly = all.features?.some(
          (f) => f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon"
        );
        if (!hasPoly && blockagePropRef.current) {
          try {
            deletePolygonFeaturesFromDraw(draw);
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
      applyBlockagePolygonToDraw(geom: GeoJSON.Geometry | null) {
        const draw = drawRef.current;
        if (!draw) return;
        try {
          deletePolygonFeaturesFromDraw(draw);
          if (!geom) {
            onBlockageChangeRef.current?.(null);
          } else if (geom.type === "Polygon" || geom.type === "MultiPolygon") {
            draw.add({
              type: "Feature",
              geometry: geom,
              properties: {},
            } as GeoJSON.Feature);
            onBlockageChangeRef.current?.(normalizeBlockageGeometry(geom));
          } else {
            onBlockageChangeRef.current?.(null);
          }
        } catch (e) {
          console.error("Failed to apply blockage polygon to draw", e);
        }
        try {
          draw.changeMode("simple_select");
        } catch {
          /* ignore */
        }
      },
      applyManualDetourLineToDraw(geom: GeoJSON.LineString | null) {
        const draw = drawRef.current;
        if (!draw) return;
        try {
          deleteLineStringFeaturesFromDraw(draw);
          if (geom?.coordinates && geom.coordinates.length >= 2) {
            draw.add({
              type: "Feature",
              geometry: geom,
              properties: {},
            } as GeoJSON.Feature);
          }
        } catch (e) {
          console.error("Failed to apply manual detour line to draw", e);
        }
      },
      startDrawDetourLine() {
        const draw = drawRef.current;
        if (!draw) return;
        try {
          // @ts-expect-error draw_line_string at runtime
          draw.changeMode("draw_line_string");
        } catch (e) {
          console.error("Failed to start draw_line_string mode", e);
        }
      },
      clearManualDetourLine() {
        const draw = drawRef.current;
        if (!draw) return;
        try {
          deleteLineStringFeaturesFromDraw(draw);
          onManualDetourLineChangeRef.current?.(null);
        } catch (e) {
          console.error("Failed to clear manual detour line", e);
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
        if (d?.path_geojson) fitBoundsFromGeoJSON(normalizeToFeatureCollection(d.path_geojson) as any);
      },
      flyTo(lat: number, lng: number, zoom = 15) {
        mapRef.current?.flyTo({ center: [lng, lat], zoom });
      },
    }),
    []
  );

  return <div ref={containerRef} className="maplibre-map-container" />;
});

export default MapLibreMap;

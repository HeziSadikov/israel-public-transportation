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

// OSM raster basemap (standard tiles).
const OSM_STYLE: maplibregl.StyleSpecification = {
  version: 8,
  sources: {
    osm: {
      type: "raster",
      tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution: "© OpenStreetMap",
      maxzoom: 19,
    },
  },
  layers: [{ id: "osm", type: "raster", source: "osm" }],
};

const SOURCE_BLOCKAGE = "blockage";
const SOURCE_ROUTE = "route";
const SOURCE_DETOUR = "detour";
const SOURCE_STOPS = "stops";
const SOURCE_PIN = "pin";

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

export type MapLibreMapHandle = {
  clearBlockage: () => void;
  cancelDrawing: () => void;
  undoLastPoint: () => void;
  fitToBlockage: () => void;
  fitToRoute: () => void;
  fitToDetour: () => void;
  flyTo: (lat: number, lng: number, zoom?: number) => void;
};

export type MapLibreMapProps = {
  /** Center as [lat, lng] (converted to [lng, lat] for MapLibre) */
  center: [number, number];
  stops: { stop_id: string; name: string; lat: number; lon: number; sequence: number }[];
  routeGeojson: GeoJSON.FeatureCollection | null;
  detour: { path_geojson?: GeoJSON.FeatureCollection | null } | null;
  blockageGeojson: GeoJSON.Geometry | null;
  onBlockageChange: (geom: GeoJSON.Geometry | null) => void;
  pinPosition: [number, number] | null;
};

const MapLibreMap = React.forwardRef<MapLibreMapHandle, MapLibreMapProps>(function MapLibreMap(
  { center, stops, routeGeojson, detour, blockageGeojson, onBlockageChange, pinPosition },
  ref
) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const drawRef = useRef<MapboxDraw | null>(null);
  const blockageRef = useRef<GeoJSON.Geometry | null>(null);
  const routeRef = useRef<GeoJSON.FeatureCollection | null>(null);
  const detourRef = useRef<MapLibreMapProps["detour"]>(null);
  const [mapReady, setMapReady] = useState(false);

  blockageRef.current = blockageGeojson;
  routeRef.current = routeGeojson;
  detourRef.current = detour;

  useEffect(() => {
    if (!containerRef.current) return;
    const [lat, lng] = center;
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: OSM_STYLE,
      center: [lng, lat],
      zoom: 9,
      minZoom: 5,
      maxZoom: 19,
      dragRotate: false,
      pitchWithRotate: false,
    });
    // Make mouse-wheel zoom a bit faster/more responsive.
    map.scrollZoom.setWheelZoomRate(1 / 180);
    map.scrollZoom.setZoomRate(1.0);
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    mapRef.current = map;

    const onLoad = () => {
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

      if (!map.getSource(SOURCE_BLOCKAGE)) {
        map.addSource(SOURCE_BLOCKAGE, { type: "geojson", data: emptyFc });
        map.addLayer({
          id: "blockage-fill",
          type: "fill",
          source: SOURCE_BLOCKAGE,
          paint: { "fill-color": "#f97316", "fill-opacity": 0.35 },
        });
        map.addLayer({
          id: "blockage-line",
          type: "line",
          source: SOURCE_BLOCKAGE,
          paint: { "line-color": "#dc2626", "line-width": 2 },
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
            "circle-radius": 6,
            "circle-color": "#1e40af",
            "circle-stroke-width": 2,
            "circle-stroke-color": "#fff",
          },
        });
      }

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

      // Draw control: polygon and rectangle only
      const draw = new MapboxDraw({
        displayControlsDefault: false,
        controls: {
          polygon: true,
          rectangle: true,
          trash: true,
        },
        defaultMode: "draw_polygon",
      });
      map.addControl(draw as any, "top-left");
      drawRef.current = draw;

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
      });
      map.on("draw.delete", () => onBlockageChange(null));

      setMapReady(true);
    };

    map.on("load", onLoad);
    return () => {
      map.remove();
      mapRef.current = null;
      drawRef.current = null;
    };
  }, [onBlockageChange]);

  // Keep Draw in sync with React blockage state (React is the source of truth)
  useEffect(() => {
    if (!mapReady) return;
    const draw = drawRef.current;
    if (!draw) return;
    // Suppress API events by default, so this does not loop back into React.
    draw.deleteAll();
    if (blockageGeojson) {
      draw.add({
        type: "Feature",
        geometry: blockageGeojson as GeoJSON.Geometry,
        properties: {},
      } as GeoJSON.Feature);
    }
  }, [mapReady, blockageGeojson]);

  // Blockage source data
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) return;
    const source = map.getSource(SOURCE_BLOCKAGE) as maplibregl.GeoJSONSource | undefined;
    if (!source) return;
    if (blockageGeojson) {
      source.setData({ type: "Feature", geometry: blockageGeojson, properties: {} } as GeoJSON.Feature);
    } else {
      source.setData({ type: "FeatureCollection", features: [] });
    }
  }, [mapReady, blockageGeojson]);

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
    const stopsFc: GeoJSONFeatureCollection = {
      type: "FeatureCollection",
      features: stops.map((s) => ({
        type: "Feature" as const,
        geometry: { type: "Point" as const, coordinates: [s.lon, s.lat] },
        properties: { stop_id: s.stop_id },
      })),
    };
    source.setData(stopsFc as any);
  }, [mapReady, stops]);

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

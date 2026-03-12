import "./app.css";

import axios from "axios";
import React, { useMemo, useRef, useState } from "react";
import { ExplorerWindow, type ExplorerTab, type GeocodeResult } from "./ExplorerWindow";
import type { AreaRouteResult, RouteInfo } from "./ExplorerWindow";
import MapLibreMap, { type MapLibreMapHandle } from "./MapLibreMap";

type StopInfo = {
  stop_id: string;
  name: string;
  lat: number;
  lon: number;
  sequence: number;
};

type GraphBuildResponse = {
  pattern_id: string;
  stop_count: number;
  edge_count: number;
  used_shape: boolean;
  used_osm_snapping: boolean;
  example_stop_ids: string[];
  feed_version: string;
};

type DetourResponse = {
  blocked_edges_count: number;
  stop_path: string[];
  path_geojson: GeoJSON.FeatureCollection;
  blocked_edges_geojson: GeoJSON.FeatureCollection;
  total_travel_time_s?: number | null;
  total_distance_m?: number | null;
  used_shape: boolean;
  used_osm_snapping: boolean;
  feed_version: string;
};

type DetourByAreaMode = "route" | "all";

type DetourByAreaRouteResult = {
  route_id: string;
  direction_id?: string | null;
  pattern_id?: string | null;
  blocked_edges_count: number;
  stop_before?: string | null;
  stop_after?: string | null;
  detour_stop_path: string[];
  detour_geojson?: GeoJSON.FeatureCollection | null;
  replaced_segment_geojson?: GeoJSON.FeatureCollection | null;
  used_transfers: boolean;
  error?: string | null;
};

type DetourByAreaResponse =
  | { mode: "route"; result: DetourByAreaRouteResult | null }
  | { mode: "all"; results: DetourByAreaRouteResult[] | null };

const API_BASE: string =
  (typeof (import.meta as any).env?.VITE_API_BASE === "string" &&
    (import.meta as any).env.VITE_API_BASE) ||
  "http://127.0.0.1:8000";

const DEFAULT_EXPLORER_POSITION = { x: 24, y: 24 };
const DEFAULT_EXPLORER_SIZE = { width: 360, height: 380 };
const EXPLORER_HEADER_HEIGHT = 44;

const App: React.FC = () => {
  const [blockageGeojson, setBlockageGeojson] = useState<GeoJSON.Geometry | null>(null);
  const [areaDate, setAreaDate] = useState(() => {
    const d = new Date();
    return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(d.getDate()).padStart(2, "0")}`;
  });
  const [areaStartTime, setAreaStartTime] = useState("04:00");
  const [areaEndTime, setAreaEndTime] = useState("23:59");
  const [areaRoutes, setAreaRoutes] = useState<AreaRouteResult[] | null>(null);
  const [areaLoading, setAreaLoading] = useState(false);
  const [selectedRoute, setSelectedRoute] = useState<RouteInfo | null>(null);
  const [selectedDirectionId, setSelectedDirectionId] = useState<string | null>(null);
  const [patternId, setPatternId] = useState<string | null>(null);
  const [stops, setStops] = useState<StopInfo[]>([]);
  const [routeGeojson, setRouteGeojson] = useState<GeoJSON.FeatureCollection | null>(null);
  const [prettyOSM, setPrettyOSM] = useState(false);
  const [routeLoading, setRouteLoading] = useState(false);
  const [detourMode, setDetourMode] = useState<DetourByAreaMode>("all");
  const [detourAreaResults, setDetourAreaResults] = useState<DetourByAreaRouteResult[] | null>(null);
  const [detourLoading, setDetourLoading] = useState(false);
  const [detour, setDetour] = useState<DetourResponse | null>(null);
  const [message, setMessage] = useState<string | null>(null);

  const [explorerOpen, setExplorerOpen] = useState(true);
  const [explorerMinimized, setExplorerMinimized] = useState(false);
  const [explorerTab, setExplorerTab] = useState<ExplorerTab>("area");
  const [explorerPosition, setExplorerPosition] = useState(DEFAULT_EXPLORER_POSITION);
  const [explorerSize, setExplorerSize] = useState(DEFAULT_EXPLORER_SIZE);
  const lastExplorerSizeRef = useRef(DEFAULT_EXPLORER_SIZE);

  const [lineSearchQuery, setLineSearchQuery] = useState("");
  const [lineSearchResults, setLineSearchResults] = useState<RouteInfo[]>([]);
  const [lineSearchLoading, setLineSearchLoading] = useState(false);

  const [addressQuery, setAddressQuery] = useState("");
  const [addressResults, setAddressResults] = useState<GeocodeResult[]>([]);
  const [addressLoading, setAddressLoading] = useState(false);

  const [pinPosition, setPinPosition] = useState<[number, number] | null>(null);

  const mapRef = useRef<MapLibreMapHandle | null>(null);

  const center: [number, number] = [31.5, 35.0];

  const loadRouteOnMap = async (routeId: string, directionId?: string | null) => {
    setRouteLoading(true);
    setMessage(null);
    setRouteGeojson(null);
    setStops([]);
    setPatternId(null);
    try {
      const buildRes = await axios.post<GraphBuildResponse>(`${API_BASE}/graph/build`, {
        route_id: routeId,
        direction_id: directionId ?? undefined,
        date: areaDate,
        pretty_osm: prettyOSM,
      });
      const pid = buildRes.data.pattern_id;
      setPatternId(pid);
      const stopsParams: Record<string, string> = { route_id: routeId, pattern_id: pid, date: areaDate };
      if (directionId != null && directionId !== "") (stopsParams as any).direction_id = directionId;
      const stopsRes = await axios.get(`${API_BASE}/graph/stops`, { params: stopsParams });
      const body = stopsRes.data as { pattern_id: string; stops: StopInfo[] };
      setStops(body.stops);
      const geoParams: Record<string, string | boolean> = {
        route_id: routeId,
        pattern_id: pid,
        date: areaDate,
        pretty_osm: prettyOSM,
      };
      if (directionId != null && directionId !== "") (geoParams as any).direction_id = directionId;
      const geoRes = await axios.get<GeoJSON.FeatureCollection>(`${API_BASE}/graph/geojson`, { params: geoParams });
      setRouteGeojson(geoRes.data);
    } catch (err) {
      console.error(err);
      setMessage("Could not load route. Check backend.");
    } finally {
      setRouteLoading(false);
    }
  };

  const handleSelectAreaRoute = (r: AreaRouteResult) => {
    const info: RouteInfo = {
      route_id: r.route_id,
      route_short_name: r.route_short_name,
      route_long_name: r.route_long_name,
      agency_id: r.agency_id ?? undefined,
      agency_name: r.agency_name ?? undefined,
    };
    setSelectedRoute(info);
    setSelectedDirectionId(r.direction_id != null ? String(r.direction_id) : null);
    setDetourMode("route");
    loadRouteOnMap(r.route_id, r.direction_id);
  };

  const handleUseForDetour = (r: AreaRouteResult) => {
    handleSelectAreaRoute(r);
  };

  const handleFindAreaRoutes = async () => {
    if (!blockageGeojson) {
      setMessage("Draw a blockage first.");
      return;
    }
    setMessage(null);
    setAreaLoading(true);
    setAreaRoutes(null);
    setDetourAreaResults(null);
    setDetour(null);
    try {
      const res = await axios.post<{ routes: AreaRouteResult[] }>(`${API_BASE}/area/routes`, {
        polygon_geojson: blockageGeojson,
        date: areaDate.trim(),
        start_time: areaStartTime.trim() || "04:00",
        end_time: areaEndTime.trim() || "23:59",
        max_results: 200,
      }, { timeout: 120000, headers: { "Content-Type": "application/json" } });
      setAreaRoutes(res.data.routes);
      if (res.data.routes.length === 0) {
        setMessage("No routes in this area for the chosen date/time.");
      }
    } catch (err: unknown) {
      console.error(err);
      setMessage(axios.isAxiosError(err) ? (err.response?.data?.detail ?? err.message) : "Error loading routes.");
      setAreaRoutes([]);
    } finally {
      setAreaLoading(false);
    }
  };

  const handleLineSearch = async () => {
    if (!lineSearchQuery.trim()) return;
    setLineSearchLoading(true);
    setMessage(null);
    try {
      const res = await axios.post<RouteInfo[]>(`${API_BASE}/routes/search`, {
        q: lineSearchQuery.trim(),
        limit: 20,
      });
      setLineSearchResults(res.data);
    } catch (e) {
      setMessage("Search failed.");
      setLineSearchResults([]);
    } finally {
      setLineSearchLoading(false);
    }
  };

  const handleSelectLineRoute = (r: RouteInfo) => {
    setSelectedRoute(r);
    setSelectedDirectionId(null);
    setDetourMode("route");
    loadRouteOnMap(r.route_id);
  };

  const handleGoToLatLng = (lat: number, lng: number) => {
    setPinPosition([lat, lng]);
    mapRef.current?.flyTo(lat, lng, 15);
  };

  const handleAddressSearch = async () => {
    if (!addressQuery.trim()) return;
    setAddressLoading(true);
    setMessage(null);
    try {
      const res = await axios.get<GeocodeResult[]>(`${API_BASE}/geocode`, {
        params: { q: addressQuery.trim(), limit: 5 },
      });
      setAddressResults(res.data);
    } catch (e) {
      setMessage("Address search failed.");
      setAddressResults([]);
    } finally {
      setAddressLoading(false);
    }
  };

  const handleSelectAddressResult = (r: GeocodeResult) => {
    setPinPosition([r.lat, r.lon]);
    mapRef.current?.flyTo(r.lat, r.lon, 16);
  };

  const handleComputeDetour = async () => {
    if (!blockageGeojson) {
      setMessage("Draw a blockage first.");
      return;
    }
    if (detourMode === "route" && !selectedRoute) {
      setMessage("Select a route in the Explorer (Area tab) or choose “Detour all”.");
      return;
    }
    setDetourLoading(true);
    setMessage(null);
    setDetour(null);
    setDetourAreaResults(null);
    try {
      const body: any = {
        mode: detourMode,
        date: areaDate.trim(),
        start_time: areaStartTime.trim() || "04:00",
        end_time: areaEndTime.trim() || "23:59",
        blockage_geojson: blockageGeojson,
        max_routes: 20,
        transfer_radius_m: 120,
      };
      if (detourMode === "route" && selectedRoute) {
        body.route_id = selectedRoute.route_id;
        body.direction_id = selectedDirectionId ?? undefined;
      }
      const res = await axios.post<DetourByAreaResponse>(`${API_BASE}/detours/by-area`, body, {
        timeout: 120000,
        headers: { "Content-Type": "application/json" },
      });
      const feedVersion = (res.data as any).feed_version ?? "";
      if (res.data.mode === "route") {
        const r = (res.data as any).result as DetourByAreaRouteResult | null;
        setDetourAreaResults(r ? [r] : []);
        if (!r) setMessage("No result for this route.");
        else if (r.error) setMessage(r.error);
        else if (r.blocked_edges_count === 0) setMessage("Route not affected by blockage.");
        else if (r.detour_geojson) {
          setDetour({
            blocked_edges_count: r.blocked_edges_count,
            stop_path: r.detour_stop_path,
            path_geojson: r.detour_geojson as any,
            blocked_edges_geojson: { type: "FeatureCollection", features: [] } as any,
            total_travel_time_s: null,
            total_distance_m: null,
            used_shape: false,
            used_osm_snapping: false,
            feed_version: feedVersion,
          });
        }
      } else {
        const all = (res.data as any).results as DetourByAreaRouteResult[] | null;
        setDetourAreaResults(all || []);
        if (!all || all.length === 0) setMessage("No affected routes for this blockage.");
        else {
          const withDetour = all.find((r) => !r.error && r.detour_geojson && r.detour_stop_path.length > 0);
          if (withDetour?.detour_geojson) {
            setDetour({
              blocked_edges_count: withDetour.blocked_edges_count,
              stop_path: withDetour.detour_stop_path,
              path_geojson: withDetour.detour_geojson as any,
              blocked_edges_geojson: { type: "FeatureCollection", features: [] } as any,
              total_travel_time_s: null,
              total_distance_m: null,
              used_shape: false,
              used_osm_snapping: false,
              feed_version: feedVersion,
            });
          }
        }
      }
    } catch (err: any) {
      console.error(err);
      setMessage(axios.isAxiosError(err) ? (err.response?.data?.detail ?? err.message) : "Detour failed.");
    } finally {
      setDetourLoading(false);
    }
  };

  const resultByRouteId = useMemo(() => {
    const m = new Map<string, DetourByAreaRouteResult>();
    (detourAreaResults ?? []).forEach((r) => {
      m.set(`${r.route_id}\t${r.direction_id ?? ""}`, r);
    });
    return m;
  }, [detourAreaResults]);

  return (
    <div className="app">
      <aside className="rail">
        <h1 className="rail-title">Detour Router</h1>

        <section className="rail-section">
          <h2 className="rail-heading">Blockage</h2>
          <div className="rail-buttons">
            <button type="button" onClick={() => mapRef.current?.startPolygon()} title="Draw polygon blockage">
              Draw polygon
            </button>
            <button type="button" onClick={() => mapRef.current?.clearBlockage()} title="Clear blockage">
              Clear
            </button>
            <button type="button" onClick={() => mapRef.current?.cancelDrawing()}>Cancel</button>
            <button type="button" onClick={() => mapRef.current?.undoLastPoint()}>Undo</button>
          </div>
        </section>

        <section className="rail-section">
          <h2 className="rail-heading">Time window</h2>
          <label>Date</label>
          <input type="text" value={areaDate} onChange={(e) => setAreaDate(e.target.value)} placeholder="YYYYMMDD" />
          <label>Start – End</label>
          <div className="row">
            <input type="text" value={areaStartTime} onChange={(e) => setAreaStartTime(e.target.value)} placeholder="04:00" />
            <input type="text" value={areaEndTime} onChange={(e) => setAreaEndTime(e.target.value)} placeholder="23:59" />
          </div>
        </section>

        <section className="rail-section">
          <h2 className="rail-heading">Detour</h2>
          <div className="radio-group">
            <label>
              <input type="radio" checked={detourMode === "route"} onChange={() => setDetourMode("route")} />
              One route
            </label>
            <label>
              <input type="radio" checked={detourMode === "all"} onChange={() => setDetourMode("all")} />
              All
            </label>
          </div>
          <button
            type="button"
            className="btn-compute"
            onClick={handleComputeDetour}
            disabled={detourLoading || !blockageGeojson}
          >
            {detourLoading ? "Computing…" : "Compute detour"}
          </button>
        </section>

        <section className="rail-section">
          <h2 className="rail-heading">Map</h2>
          <div className="rail-buttons">
            <button type="button" onClick={() => mapRef.current?.fitToBlockage()} disabled={!blockageGeojson}>
              Fit blockage
            </button>
            <button type="button" onClick={() => mapRef.current?.fitToRoute()} disabled={!routeGeojson}>
              Fit route
            </button>
            <button type="button" onClick={() => mapRef.current?.fitToDetour()} disabled={!detour}>
              Fit detour
            </button>
          </div>
        </section>

        <section className="rail-section">
          <button
            type="button"
            className="btn-explorer-toggle"
            onClick={() => { setExplorerOpen((o) => !o); if (!explorerOpen) setExplorerMinimized(false); }}
          >
            {explorerOpen ? "Hide explorer" : "Show explorer"}
          </button>
        </section>

        {message && <div className="rail-status">{message}</div>}
      </aside>

      <div className="map-container">
        <MapLibreMap
          ref={mapRef}
          center={center}
          stops={stops}
          routeGeojson={routeGeojson as GeoJSON.FeatureCollection | null}
          detour={detour as { path_geojson?: GeoJSON.FeatureCollection | null } | null}
          blockageGeojson={blockageGeojson as GeoJSON.Geometry | null}
          onBlockageChange={setBlockageGeojson}
          pinPosition={pinPosition}
        />
        {explorerOpen && (
          <ExplorerWindow
            activeTab={explorerTab}
            onTabChange={setExplorerTab}
            isOpen={true}
            isMinimized={explorerMinimized}
            onMinimize={() => {
              setExplorerMinimized((m) => {
                const next = !m;
                if (next) {
                  lastExplorerSizeRef.current = explorerSize;
                  setExplorerSize((s) => ({ ...s, height: EXPLORER_HEADER_HEIGHT }));
                } else {
                  setExplorerSize(lastExplorerSizeRef.current);
                }
                return next;
              });
            }}
            onClose={() => setExplorerOpen(false)}
            position={explorerPosition}
            size={explorerSize}
            onPositionChange={setExplorerPosition}
            onSizeChange={setExplorerSize}
            areaRoutes={areaRoutes}
            areaLoading={areaLoading}
            resultByRouteId={resultByRouteId}
            selectedRouteId={selectedRoute?.route_id ?? null}
            onFindAreaRoutes={handleFindAreaRoutes}
            onSelectAreaRoute={handleSelectAreaRoute}
            onFitToRoute={() => mapRef.current?.fitToRoute()}
            onUseForDetour={handleUseForDetour}
            hasBlockage={!!blockageGeojson}
            lineSearchQuery={lineSearchQuery}
            onLineSearchQueryChange={setLineSearchQuery}
            lineSearchResults={lineSearchResults}
            lineSearchLoading={lineSearchLoading}
            onLineSearch={handleLineSearch}
            onSelectLineRoute={handleSelectLineRoute}
            onGoToLatLng={handleGoToLatLng}
            addressQuery={addressQuery}
            onAddressQueryChange={setAddressQuery}
            addressResults={addressResults}
            addressLoading={addressLoading}
            onAddressSearch={handleAddressSearch}
            onSelectAddressResult={handleSelectAddressResult}
          />
        )}
      </div>
    </div>
  );
};

export default App;

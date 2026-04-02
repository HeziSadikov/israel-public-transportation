import "./app.css";

import axios from "axios";
import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  ExplorerWindow,
  type ExplorerSortBy,
  type ExplorerTab,
  type GeocodeResult,
  type SelectedStopInfo,
  type StopLineResult,
} from "./ExplorerWindow";
import type { AreaRouteResult, RouteInfo } from "./ExplorerWindow";
import MapLibreMap, { type BasemapKind, type MapLibreMapHandle } from "./MapLibreMap";

type StopInfo = {
  stop_id: string;
  name: string;
  stop_code?: string | null;
  lat: number;
  lon: number;
  sequence: number;
};

type GraphPreviewResponse = {
  pattern_id: string;
  stops: StopInfo[];
  route_geojson: GeoJSON.FeatureCollection;
  used_osm_snapping: boolean;
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

type StopRoutesResponse = {
  stop_id: string;
  routes: StopLineResult[];
};

type StopSearchResponseItem = {
  stop_id: string;
  stop_name?: string | null;
  stop_code?: string | null;
  stop_lat: number;
  stop_lon: number;
};

const API_BASE: string =
  (typeof (import.meta as any).env?.VITE_API_BASE === "string" &&
    (import.meta as any).env.VITE_API_BASE) ||
  "http://127.0.0.1:8000";

const DEFAULT_EXPLORER_POSITION = { x: 24, y: 24 };
const DEFAULT_EXPLORER_SIZE = { width: 620, height: 420 };

const HHMM_RE = /^([01]\d|2[0-3]):([0-5]\d)$/;
const YMD_RE = /^\d{8}$/;

const toIsoDate = (yyyymmdd: string): string => {
  if (!YMD_RE.test(yyyymmdd)) return "";
  return `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}`;
};

const fromIsoDate = (isoDate: string): string => isoDate.replaceAll("-", "");

const isValidYmd = (yyyymmdd: string): boolean => {
  if (!YMD_RE.test(yyyymmdd)) return false;
  const iso = toIsoDate(yyyymmdd);
  const d = new Date(`${iso}T00:00:00`);
  if (Number.isNaN(d.getTime())) return false;
  return (
    d.getFullYear() === Number(yyyymmdd.slice(0, 4)) &&
    d.getMonth() + 1 === Number(yyyymmdd.slice(4, 6)) &&
    d.getDate() === Number(yyyymmdd.slice(6, 8))
  );
};

const toLocalDateTime = (yyyymmdd: string, hhmm: string): Date | null => {
  if (!isValidYmd(yyyymmdd) || !HHMM_RE.test(hhmm)) return null;
  const y = Number(yyyymmdd.slice(0, 4));
  const m = Number(yyyymmdd.slice(4, 6)) - 1;
  const d = Number(yyyymmdd.slice(6, 8));
  const hh = Number(hhmm.slice(0, 2));
  const mm = Number(hhmm.slice(3, 5));
  const dt = new Date(y, m, d, hh, mm, 0, 0);
  if (Number.isNaN(dt.getTime())) return null;
  return dt;
};

const App: React.FC = () => {
  const [blockageGeojson, setBlockageGeojson] = useState<GeoJSON.Geometry | null>(null);
  const [areaStartDate, setAreaStartDate] = useState(() => {
    const d = new Date();
    return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(d.getDate()).padStart(2, "0")}`;
  });
  const [areaEndDate, setAreaEndDate] = useState(() => {
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
  const [useOSMDetour, setUseOSMDetour] = useState(false);

  const [explorerOpen, setExplorerOpen] = useState(true);
  const [explorerTab, setExplorerTab] = useState<ExplorerTab>("area");
  const [explorerPosition, setExplorerPosition] = useState(DEFAULT_EXPLORER_POSITION);
  const [explorerSize, setExplorerSize] = useState(DEFAULT_EXPLORER_SIZE);

  const [lineSearchQuery, setLineSearchQuery] = useState("");
  const [lineSearchResults, setLineSearchResults] = useState<RouteInfo[]>([]);
  const [lineSearchLoading, setLineSearchLoading] = useState(false);
  const [sortBy, setSortBy] = useState<ExplorerSortBy>("line_asc");
  const [stopSearchQuery, setStopSearchQuery] = useState("");
  const [selectedStop, setSelectedStop] = useState<SelectedStopInfo | null>(null);
  const [stopSearchResults, setStopSearchResults] = useState<SelectedStopInfo[]>([]);
  const [stopSearchLoading, setStopSearchLoading] = useState(false);
  const [stopSearchError, setStopSearchError] = useState<string | null>(null);
  const [stopLinesLoading, setStopLinesLoading] = useState(false);
  const [stopLinesResults, setStopLinesResults] = useState<StopLineResult[]>([]);
  const [stopLinesError, setStopLinesError] = useState<string | null>(null);
  const [stopLinesHint, setStopLinesHint] = useState<string | null>(null);

  const [addressQuery, setAddressQuery] = useState("");
  const [addressResults, setAddressResults] = useState<GeocodeResult[]>([]);
  const [addressLoading, setAddressLoading] = useState(false);

  const [pinPosition, setPinPosition] = useState<[number, number] | null>(null);
  const [basemap, setBasemap] = useState<BasemapKind>("osm");

  const mapRef = useRef<MapLibreMapHandle | null>(null);
  const latestRouteLoadIdRef = useRef(0);
  const stopLinesAbortRef = useRef<AbortController | null>(null);
  const stopLinesRequestIdRef = useRef(0);
  const timeStartDateInputRef = useRef<HTMLInputElement | null>(null);
  const timeStartInputRef = useRef<HTMLInputElement | null>(null);
  const timeEndDateInputRef = useRef<HTMLInputElement | null>(null);
  const timeEndInputRef = useRef<HTMLInputElement | null>(null);

  const center: [number, number] = [31.5, 35.0];
  const timeWindowError = useMemo(() => {
    if (!isValidYmd(areaStartDate.trim())) return "Start date must be valid and formatted as YYYYMMDD.";
    if (!isValidYmd(areaEndDate.trim())) return "End date must be valid and formatted as YYYYMMDD.";
    if (!HHMM_RE.test(areaStartTime.trim())) return "Start time must be in HH:MM (24h).";
    if (!HHMM_RE.test(areaEndTime.trim())) return "End time must be in HH:MM (24h).";
    const startDt = toLocalDateTime(areaStartDate.trim(), areaStartTime.trim());
    const endDt = toLocalDateTime(areaEndDate.trim(), areaEndTime.trim());
    if (!startDt || !endDt) return "Invalid date/time value.";
    if (endDt.getTime() < startDt.getTime()) return "End date/time must be later than or equal to start date/time.";
    return null;
  }, [areaStartDate, areaEndDate, areaStartTime, areaEndTime]);
  const isTimeWindowValid = timeWindowError == null;

  const timePresets: { label: string; start: string; end: string }[] = [
    { label: "All day", start: "04:00", end: "23:59" },
    { label: "AM", start: "06:00", end: "10:00" },
    { label: "Midday", start: "10:00", end: "16:00" },
    { label: "PM", start: "16:00", end: "22:00" },
  ];

  const applyRelativePreset = (hoursAhead: number) => {
    const now = new Date();
    const end = new Date(now.getTime() + hoursAhead * 60 * 60 * 1000);
    const toHHMM = (d: Date) => `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
    const toYMD = (d: Date) =>
      `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(d.getDate()).padStart(2, "0")}`;
    setAreaStartDate(toYMD(now));
    setAreaEndDate(toYMD(end));
    setAreaStartTime(toHHMM(now));
    setAreaEndTime(toHHMM(end));
  };

  const loadRouteOnMap = async (routeId: string, directionId?: string | null) => {
    const loadId = ++latestRouteLoadIdRef.current;
    setRouteLoading(true);
    setMessage("Loading route preview...");
    setStops([]);
    setPatternId(null);
    try {
      const previewParams: Record<string, string | boolean> = {
        route_id: routeId,
        date: areaStartDate,
        pretty_osm: prettyOSM,
      };
      if (directionId != null && directionId !== "") (previewParams as any).direction_id = directionId;
      const t0 = performance.now();
      const previewRes = await axios.get<GraphPreviewResponse>(`${API_BASE}/graph/preview`, {
        params: previewParams,
      });
      const t1 = performance.now();
      if (loadId !== latestRouteLoadIdRef.current) return;
      const preview = previewRes.data;
      setPatternId(preview.pattern_id);
      setRouteGeojson(preview.route_geojson);
      setStops(preview.stops || []);
      // Include backend-timing headers when available.
      const backendMs = Number(previewRes.headers?.["x-elapsed-ms"] || 0);
      const previewHit = previewRes.headers?.["x-cache-hit"] ?? "n/a";
      const graphHit = previewRes.headers?.["x-graph-cache-hit"] ?? "none";
      const frontendMs = t1 - t0;
      console.info(
        `[route/preview] route_id=${routeId} direction_id=${directionId ?? ""} frontend_ms=${frontendMs.toFixed(
          1
        )} backend_ms=${backendMs.toFixed(1)} preview_hit=${previewHit} graph_hit=${graphHit}`
      );
      setMessage(
        `Route loaded: frontend ${frontendMs.toFixed(0)}ms, backend ${backendMs.toFixed(0)}ms, preview=${previewHit}, graph=${graphHit} (${preview.stops?.length ?? 0} stops)`
      );
      requestAnimationFrame(() => {
        const tPaint = performance.now();
        console.info(`[route/preview] paint_after_ms=${(tPaint - t0).toFixed(1)}`);
      });
      setTimeout(() => {
        if (loadId === latestRouteLoadIdRef.current) setMessage(null);
      }, 2500);
    } catch (err) {
      if (loadId !== latestRouteLoadIdRef.current) return;
      console.error(err);
      setMessage("Could not load route. Check backend.");
    } finally {
      if (loadId !== latestRouteLoadIdRef.current) return;
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
    if (!isTimeWindowValid) {
      setMessage(timeWindowError);
      return;
    }
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
        start_date: areaStartDate.trim(),
        start_time: areaStartTime.trim() || "04:00",
        end_date: areaEndDate.trim(),
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
    if (!isTimeWindowValid) {
      setMessage(timeWindowError);
      return;
    }
    if (!lineSearchQuery.trim()) return;
    setLineSearchLoading(true);
    setMessage(null);
    try {
      const res = await axios.post<RouteInfo[]>(`${API_BASE}/routes/search`, {
        q: lineSearchQuery.trim(),
        limit: 20,
        start_date: areaStartDate.trim(),
        start_time: areaStartTime.trim() || "04:00",
        end_date: areaEndDate.trim(),
        end_time: areaEndTime.trim() || "23:59",
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

  useEffect(() => {
    const q = stopSearchQuery.trim();
    if (q.length < 2) {
      setStopSearchResults([]);
      setStopSearchError(null);
      setStopSearchLoading(false);
      return;
    }
    const controller = new AbortController();
    const timer = setTimeout(async () => {
      setStopSearchLoading(true);
      setStopSearchError(null);
      try {
        const res = await axios.get<StopSearchResponseItem[]>(`${API_BASE}/stops/search`, {
          params: { q, limit: 25 },
          signal: controller.signal,
        });
        setStopSearchResults(
          (res.data || []).map((s) => ({
            stop_id: s.stop_id,
            stop_name: s.stop_name ?? undefined,
            stop_code: s.stop_code ?? null,
            lat: s.stop_lat,
            lon: s.stop_lon,
          }))
        );
      } catch (err) {
        if (axios.isCancel(err)) return;
        setStopSearchResults([]);
        setStopSearchError(
          axios.isAxiosError(err) ? (err.response?.data?.detail ?? err.message) : "Stop search failed."
        );
      } finally {
        setStopSearchLoading(false);
      }
    }, 250);
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [stopSearchQuery]);

  const handleSelectStopResult = (s: SelectedStopInfo) => {
    setSelectedStop(s);
    setStopSearchQuery(s.stop_id);
    setStopSearchResults([]);
    setStopSearchError(null);
    if (typeof s.lat === "number" && typeof s.lon === "number") {
      mapRef.current?.flyTo(s.lat, s.lon, 16);
    }
  };

  const handleMapStopClick = (s: SelectedStopInfo) => {
    setSelectedStop(s);
    setStopSearchQuery(s.stop_id);
    setStopSearchResults([]);
    setStopSearchError(null);
    setExplorerTab("stop");
  };

  const handleOpenStopInExplorer = (s: SelectedStopInfo) => {
    setSelectedStop(s);
    setStopSearchQuery(s.stop_id);
    setStopSearchResults([]);
    setStopSearchError(null);
    setExplorerTab("stop");
    setExplorerOpen(true);
  };

  useEffect(() => {
    stopLinesAbortRef.current?.abort();
    stopLinesAbortRef.current = null;
    setStopLinesResults([]);
    setStopLinesError(null);
    setStopLinesHint(null);
    setStopLinesLoading(false);
  }, [selectedStop?.stop_id]);

  const handleSearchLinesInStop = async () => {
    if (!isTimeWindowValid) {
      setStopLinesError(timeWindowError);
      return;
    }
    if (!selectedStop?.stop_id) return;
    stopLinesAbortRef.current?.abort();
    const controller = new AbortController();
    stopLinesAbortRef.current = controller;
    const requestId = ++stopLinesRequestIdRef.current;
    setStopLinesLoading(true);
    setStopLinesError(null);
    setStopLinesHint(null);
    setStopLinesResults([]);
    const slowHintTimer = window.setTimeout(() => {
      if (requestId === stopLinesRequestIdRef.current) {
        setStopLinesHint("Searching large schedule…");
      }
    }, 2000);
    try {
      const res = await axios.post<StopRoutesResponse>(`${API_BASE}/stop/routes`, {
        stop_id: selectedStop.stop_id,
        start_date: areaStartDate.trim(),
        start_time: areaStartTime.trim() || "04:00",
        end_date: areaEndDate.trim(),
        end_time: areaEndTime.trim() || "23:59",
        max_results: 100,
      }, {
        signal: controller.signal,
      });
      if (requestId !== stopLinesRequestIdRef.current) return;
      setStopLinesResults(res.data.routes || []);
      if ((res.data.routes || []).length === 0) {
        setStopLinesError("No lines found for this stop in the selected time window.");
      }
    } catch (err) {
      if (requestId !== stopLinesRequestIdRef.current) return;
      if (axios.isCancel(err)) return;
      setStopLinesError(
        axios.isAxiosError(err) ? (err.response?.data?.detail ?? err.message) : "Stop lines search failed."
      );
    } finally {
      window.clearTimeout(slowHintTimer);
      if (requestId !== stopLinesRequestIdRef.current) return;
      setStopLinesHint(null);
      setStopLinesLoading(false);
      if (stopLinesAbortRef.current === controller) {
        stopLinesAbortRef.current = null;
      }
    }
  };

  const handleSelectStopLineRoute = (r: StopLineResult) => {
    const info: RouteInfo = {
      route_id: r.route_id,
      route_short_name: r.route_short_name ?? undefined,
      route_long_name: r.route_long_name ?? undefined,
      agency_name: r.agency_name ?? undefined,
    };
    setSelectedRoute(info);
    setSelectedDirectionId(r.direction_id != null ? String(r.direction_id) : null);
    setDetourMode("route");
    loadRouteOnMap(r.route_id, r.direction_id ?? null);
  };

  const handleComputeDetour = async () => {
    if (!isTimeWindowValid) {
      setMessage(timeWindowError);
      return;
    }
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
        start_date: areaStartDate.trim(),
        start_time: areaStartTime.trim() || "04:00",
        end_date: areaEndDate.trim(),
        end_time: areaEndTime.trim() || "23:59",
        blockage_geojson: blockageGeojson,
        max_routes: 20,
        transfer_radius_m: 250,
        use_osm_detour: useOSMDetour,
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

        <section className="rail-section time-window-section">
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

        <section className="rail-section time-window-section">
          <h2 className="rail-heading">Time window</h2>
          <label>Start date/time</label>
          <div className="row">
            <input
              ref={timeStartDateInputRef}
              type="date"
              value={toIsoDate(areaStartDate.trim())}
              onChange={(e) => setAreaStartDate(fromIsoDate(e.target.value))}
              onKeyDown={(e) => {
                if (e.key !== "Enter") return;
                e.preventDefault();
                timeStartInputRef.current?.focus();
              }}
            />
            <input
              ref={timeStartInputRef}
              type="time"
              value={HHMM_RE.test(areaStartTime.trim()) ? areaStartTime.trim() : ""}
              onChange={(e) => setAreaStartTime(e.target.value)}
              onKeyDown={(e) => {
                if (e.key !== "Enter") return;
                e.preventDefault();
                timeEndDateInputRef.current?.focus();
              }}
            />
          </div>
          <label>End date/time</label>
          <div className="row">
            <input
              ref={timeEndDateInputRef}
              type="date"
              value={toIsoDate(areaEndDate.trim())}
              onChange={(e) => setAreaEndDate(fromIsoDate(e.target.value))}
              onKeyDown={(e) => {
                if (e.key !== "Enter") return;
                e.preventDefault();
                timeEndInputRef.current?.focus();
              }}
            />
            <input
              ref={timeEndInputRef}
              type="time"
              value={HHMM_RE.test(areaEndTime.trim()) ? areaEndTime.trim() : ""}
              onChange={(e) => setAreaEndTime(e.target.value)}
              onKeyDown={(e) => {
                if (e.key !== "Enter") return;
                e.preventDefault();
                timeStartDateInputRef.current?.focus();
              }}
            />
          </div>
          <div className="time-window-presets">
            {timePresets.map((preset) => (
              <button
                key={preset.label}
                type="button"
                className="btn-time-preset"
                onClick={() => {
                  setAreaStartTime(preset.start);
                  setAreaEndTime(preset.end);
                }}
              >
                {preset.label}
              </button>
            ))}
            <button type="button" className="btn-time-preset" onClick={() => applyRelativePreset(1)}>
              +1h
            </button>
            <button type="button" className="btn-time-preset" onClick={() => applyRelativePreset(2)}>
              +2h
            </button>
          </div>
          {timeWindowError ? (
            <p className="time-window-validation error">{timeWindowError}</p>
          ) : (
            <p className="time-window-validation">Time window looks good.</p>
          )}
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
          <label style={{ display: "block", marginTop: 8 }}>
            <input
              type="checkbox"
              checked={useOSMDetour}
              onChange={(e) => setUseOSMDetour(e.target.checked)}
            />{" "}
            Use road-network detour (Valhalla)
          </label>
          <button
            type="button"
            className="btn-compute"
            onClick={handleComputeDetour}
            disabled={detourLoading || !blockageGeojson || !isTimeWindowValid}
          >
            {detourLoading ? "Computing…" : "Compute detour"}
          </button>
        </section>

        <section className="rail-section">
          <h2 className="rail-heading">Sort by</h2>
          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value as ExplorerSortBy)}
            className="rail-select"
          >
            <option value="line_asc">Line number (small to big)</option>
            <option value="line_desc">Line number (big to small)</option>
            <option value="agency_asc">Agency (A to Z)</option>
            <option value="agency_desc">Agency (Z to A)</option>
            <option value="trips_desc">Trips count (high to low)</option>
            <option value="destination_asc">Destination (A to Z)</option>
          </select>
        </section>

        <section className="rail-section">
          <h2 className="rail-heading">Map</h2>
          <label>Basemap</label>
          <select
            className="rail-select"
            value={basemap}
            onChange={(e) => setBasemap(e.target.value as BasemapKind)}
            title="Raster options use GPU tuning for clarity; vector uses OpenFreeMap (crisp labels)"
          >
            <option value="osm">OSM standard (raster)</option>
            <option value="carto_light">Carto light (minimal raster)</option>
            <option value="vector_liberty">Vector Liberty (OpenFreeMap)</option>
          </select>
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
            onClick={() => setExplorerOpen((o) => !o)}
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
          selectedStopId={selectedStop?.stop_id ?? null}
          basemap={basemap}
          onStopClick={handleMapStopClick}
          onStopOpenInExplorer={handleOpenStopInExplorer}
        />
        {explorerOpen && (
          <ExplorerWindow
            activeTab={explorerTab}
            onTabChange={setExplorerTab}
            isOpen={true}
            onMinimize={() => setExplorerOpen((o) => !o)}
            onClose={() => setExplorerOpen(false)}
            position={explorerPosition}
            size={explorerSize}
            onPositionChange={setExplorerPosition}
            onSizeChange={setExplorerSize}
            areaRoutes={areaRoutes}
            areaLoading={areaLoading}
            timeWindowValid={isTimeWindowValid}
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
            stopSearchQuery={stopSearchQuery}
            onStopSearchQueryChange={setStopSearchQuery}
            stopSearchResults={stopSearchResults}
            stopSearchLoading={stopSearchLoading}
            stopSearchError={stopSearchError}
            onSelectStopResult={handleSelectStopResult}
            selectedStop={selectedStop}
            stopLinesLoading={stopLinesLoading}
            stopLinesResults={stopLinesResults}
            stopLinesError={stopLinesError}
            stopLinesHint={stopLinesHint}
            onSearchLinesInStop={handleSearchLinesInStop}
            onSelectStopLineRoute={handleSelectStopLineRoute}
            sortBy={sortBy}
          />
        )}
      </div>
    </div>
  );
};

export default App;

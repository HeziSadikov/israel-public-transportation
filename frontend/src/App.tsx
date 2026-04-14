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
import { isGovmapBasemapConfigured } from "./govmapBasemapEnv";

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

type DetourTurnStep = {
  instruction_he?: string | null;
  instruction_en?: string | null;
  street?: string | null;
  toward_street?: string | null;
  intersection_with?: string | null;
  turn?: string | null;
  distance_m?: number | null;
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
  turn_by_turn?: DetourTurnStep[] | null;
  from_override?: boolean;
  instructions_only?: boolean;
  reason_code?: string | null;
  strategy_used?: string | null;
  confidence?: number | null;
  diagnostics?: Record<string, unknown> | null;
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
  turn_by_turn?: DetourTurnStep[] | null;
  from_override?: boolean;
  instructions_only?: boolean;
  reason_code?: string | null;
  strategy_used?: string | null;
  confidence?: number | null;
  diagnostics?: Record<string, unknown> | null;
};

type DetourByAreaResponse =
  | { mode: "route"; result: DetourByAreaRouteResult | null; feed_version?: string | null }
  | { mode: "all"; results: DetourByAreaRouteResult[] | null; feed_version?: string | null };

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
  "http://127.0.0.1:8000/api/v1";

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

/** Clamp YYYYMMDD string to loaded GTFS calendar span when min/max are known (from /feed/status). */
const clampYmdToCalendar = (yyyymmdd: string, calMin: number | null, calMax: number | null): string => {
  if (calMin == null || calMax == null || !YMD_RE.test(yyyymmdd)) return yyyymmdd;
  let n = parseInt(yyyymmdd, 10);
  if (Number.isNaN(n)) return yyyymmdd;
  if (n < calMin) n = calMin;
  if (n > calMax) n = calMax;
  return String(n).padStart(8, "0");
};

const formatYmdInt = (n: number): string => {
  const s = String(n).padStart(8, "0");
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
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
  const [feedCalendarNotice, setFeedCalendarNotice] = useState<string | null>(null);
  const [feedCalMin, setFeedCalMin] = useState<number | null>(null);
  const [feedCalMax, setFeedCalMax] = useState<number | null>(null);
  const [useOSMDetour, setUseOSMDetour] = useState(false);
  const [detourRoutingEngine, setDetourRoutingEngine] = useState<"astar" | "dijkstra">("astar");
  const [manualDetourOpen, setManualDetourOpen] = useState(false);
  const [instructionsTextHe, setInstructionsTextHe] = useState("");
  const [manualGeoAdvancedOpen, setManualGeoAdvancedOpen] = useState(false);
  /** Lon/lat LineString from map draw or pasted GeoJSON; sent as detour_road_geojson when applying. */
  const [manualDetourDraftLine, setManualDetourDraftLine] = useState<GeoJSON.LineString | null>(null);
  const [manualRoadGeoJsonText, setManualRoadGeoJsonText] = useState("");
  const [manualTurnRows, setManualTurnRows] = useState<{ instruction_he: string; instruction_en: string }[]>([
    { instruction_he: "", instruction_en: "" },
  ]);
  const [rememberStreetOverride, setRememberStreetOverride] = useState(false);
  const [manualStopBefore, setManualStopBefore] = useState("");
  const [manualStopAfter, setManualStopAfter] = useState("");
  const [showDiagnostics, setShowDiagnostics] = useState(false);

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
  const [drawMode, setDrawMode] = useState<string>("simple_select");
  const govmapBasemapAvailable = isGovmapBasemapConfigured();

  useEffect(() => {
    if (basemap === "govmap" && !govmapBasemapAvailable) {
      setBasemap("osm");
    }
  }, [basemap, govmapBasemapAvailable]);

  const mapRef = useRef<MapLibreMapHandle | null>(null);

  const handleManualDetourLineFromMap = (geom: GeoJSON.LineString | null) => {
    setManualDetourDraftLine(geom);
    if (geom) {
      setManualRoadGeoJsonText(JSON.stringify(geom, null, 2));
    } else {
      setManualRoadGeoJsonText("");
    }
  };
  const latestRouteLoadIdRef = useRef(0);
  const stopLinesAbortRef = useRef<AbortController | null>(null);
  const stopLinesRequestIdRef = useRef(0);
  const timeStartDateInputRef = useRef<HTMLInputElement | null>(null);
  const timeStartInputRef = useRef<HTMLInputElement | null>(null);
  const timeEndDateInputRef = useRef<HTMLInputElement | null>(null);
  const timeEndInputRef = useRef<HTMLInputElement | null>(null);

  const center: [number, number] = [31.5, 35.0];

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await axios.get<{
          calendar_min_ymd?: number | null;
          calendar_max_ymd?: number | null;
          calendar_coverage_note?: string | null;
        }>(`${API_BASE}/feed/status`);
        if (cancelled) return;
        const mn = res.data.calendar_min_ymd ?? null;
        const mx = res.data.calendar_max_ymd ?? null;
        setFeedCalMin(mn);
        setFeedCalMax(mx);
        if (mn != null && mx != null) {
          setAreaStartDate((prev) => clampYmdToCalendar(prev, mn, mx));
          setAreaEndDate((prev) => clampYmdToCalendar(prev, mn, mx));
        }
        const note = res.data.calendar_coverage_note?.trim();
        if (note) setFeedCalendarNotice(note);
      } catch {
        /* offline or CORS — keep defaults */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

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
    setAreaStartDate(clampYmdToCalendar(toYMD(now), feedCalMin, feedCalMax));
    setAreaEndDate(clampYmdToCalendar(toYMD(end), feedCalMin, feedCalMax));
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
      const res = await axios.post<{
        routes: AreaRouteResult[];
        calendar_hint?: string | null;
      }>(`${API_BASE}/area/routes`, {
        polygon_geojson: blockageGeojson,
        start_date: areaStartDate.trim(),
        start_time: areaStartTime.trim() || "04:00",
        end_date: areaEndDate.trim(),
        end_time: areaEndTime.trim() || "23:59",
        max_results: 200,
      }, { timeout: 600000, headers: { "Content-Type": "application/json" } });
      setAreaRoutes(res.data.routes);
      if (res.data.routes.length === 0) {
        const hint = res.data.calendar_hint?.trim();
        const base = hint || "No routes in this area for the chosen date/time.";
        const spanExtra =
          !hint && feedCalMin != null && feedCalMax != null
            ? ` Loaded GTFS calendar: ${formatYmdInt(feedCalMin)} to ${formatYmdInt(feedCalMax)}.`
            : "";
        setMessage(base + spanExtra);
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

  const mapAreaResultToDetour = (r: DetourByAreaRouteResult, feedVersion: string): DetourResponse => {
    const raw = r.detour_geojson as GeoJSON.FeatureCollection | GeoJSON.Feature | null | undefined;
    const asFeatureCollection: GeoJSON.FeatureCollection =
      raw && (raw as GeoJSON.FeatureCollection).type === "FeatureCollection"
        ? (raw as GeoJSON.FeatureCollection)
        : raw && (raw as GeoJSON.Feature).type === "Feature"
          ? { type: "FeatureCollection", features: [raw as GeoJSON.Feature] }
          : { type: "FeatureCollection", features: [] };
    return {
    blocked_edges_count: r.blocked_edges_count,
    stop_path: r.detour_stop_path,
    path_geojson: asFeatureCollection,
    blocked_edges_geojson: { type: "FeatureCollection", features: [] } as GeoJSON.FeatureCollection,
    total_travel_time_s: null,
    total_distance_m: null,
    used_shape: false,
    used_osm_snapping: false,
    feed_version: feedVersion,
    turn_by_turn: r.turn_by_turn ?? null,
    from_override: r.from_override ?? false,
    instructions_only: r.instructions_only ?? false,
    reason_code: r.reason_code ?? null,
    strategy_used: r.strategy_used ?? null,
    confidence: r.confidence ?? null,
    diagnostics: r.diagnostics ?? null,
    };
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
      const body: Record<string, unknown> = {
        mode: detourMode,
        start_date: areaStartDate.trim(),
        start_time: areaStartTime.trim() || "04:00",
        end_date: areaEndDate.trim(),
        end_time: areaEndTime.trim() || "23:59",
        blockage_geojson: blockageGeojson,
        max_routes: 20,
        transfer_radius_m: 250,
        use_osm_detour: useOSMDetour,
        routing_engine: detourRoutingEngine,
      };
      if (detourMode === "route" && selectedRoute) {
        body.route_id = selectedRoute.route_id;
        body.direction_id = selectedDirectionId ?? undefined;
      }
      const res = await axios.post<DetourByAreaResponse>(`${API_BASE}/detours/by-area`, body, {
        timeout: 120000,
        headers: { "Content-Type": "application/json" },
      });
      const feedVersion = res.data.feed_version ?? "";
      if (res.data.mode === "route") {
        const r = res.data.result;
        setDetourAreaResults(r ? [r] : []);
        if (!r) setMessage("No result for this route.");
        else if (r.error) {
          setMessage(r.error);
          if (r.stop_before) setManualStopBefore(r.stop_before);
          if (r.stop_after) setManualStopAfter(r.stop_after);
        }         else if (r.blocked_edges_count === 0) setMessage("Route not affected by blockage.");
        else if (
          r.detour_geojson ||
          (r.instructions_only && r.turn_by_turn && r.turn_by_turn.length > 0)
        ) {
          setDetour(mapAreaResultToDetour(r, feedVersion));
          if (r.stop_before) setManualStopBefore(r.stop_before);
          if (r.stop_after) setManualStopAfter(r.stop_after);
          if (r.instructions_only && r.turn_by_turn && r.turn_by_turn.length > 0) {
            setMessage("Computed instructions-only fallback. No mapped detour geometry was returned.");
          } else if (r.strategy_used === "gtfs_multiroute" && r.reason_code === "gtfs_only_fallback") {
            setMessage("Detour uses GTFS fallback geometry. It may not reflect drivable roads exactly.");
          } else if (r.strategy_used === "gtfs_road_hybrid") {
            setMessage("Detour selected with GTFS + road validation.");
          }
        } else {
          const reason = r.reason_code ? ` (${r.reason_code})` : "";
          const strategy = r.strategy_used ? ` via ${r.strategy_used}` : "";
          setMessage(`Detour could not produce mappable geometry${strategy}${reason}.`);
        }
      } else {
        const all = res.data.results;
        setDetourAreaResults(all || []);
        if (!all || all.length === 0) setMessage("No affected routes for this blockage.");
        else {
          const withDetour = all.find(
            (r) =>
              !r.error &&
              (Boolean(r.detour_geojson) || Boolean(r.instructions_only && (r.turn_by_turn?.length ?? 0) > 0)) &&
              r.detour_stop_path.length > 0
          );
          if (withDetour?.detour_geojson) {
            setDetour(mapAreaResultToDetour(withDetour, feedVersion));
            if (withDetour.strategy_used === "gtfs_multiroute" && withDetour.reason_code === "gtfs_only_fallback") {
              setMessage("Showing GTFS fallback detour geometry for one affected route.");
            } else if (withDetour.strategy_used === "gtfs_road_hybrid") {
              setMessage("Showing a GTFS + road validated detour.");
            }
          } else if (withDetour?.instructions_only) {
            setDetour(mapAreaResultToDetour(withDetour, feedVersion));
            setMessage("Using instructions-only fallback for one affected route.");
          } else {
            setMessage("Affected routes were found, but none returned drawable detour geometry.");
          }
        }
      }
    } catch (err: unknown) {
      console.error(err);
      setMessage(axios.isAxiosError(err) ? (err.response?.data?.detail ?? err.message) : "Detour failed.");
    } finally {
      setDetourLoading(false);
    }
  };

  const handleApplyManualStreetDetour = async () => {
    if (!isTimeWindowValid) {
      setMessage(timeWindowError);
      return;
    }
    if (!blockageGeojson) {
      setMessage("Draw a blockage first.");
      return;
    }
    if (!selectedRoute) {
      setMessage("Select one route (switch to “One route”) and try again.");
      return;
    }
    let road: GeoJSON.Geometry | undefined;
    if (manualDetourDraftLine?.coordinates && manualDetourDraftLine.coordinates.length >= 2) {
      road = manualDetourDraftLine;
    } else {
      const geoTrim = manualRoadGeoJsonText.trim();
      if (geoTrim) {
        try {
          const parsed = JSON.parse(geoTrim) as unknown;
          if (parsed && typeof parsed === "object" && (parsed as GeoJSON.Geometry).type) {
            road = parsed as GeoJSON.Geometry;
          } else {
            setMessage("Advanced: road field must be a GeoJSON geometry object (e.g. LineString).");
            return;
          }
        } catch {
          setMessage("Advanced: invalid JSON for road geometry.");
          return;
        }
      }
    }
    const turn_by_turn = manualTurnRows
      .map((row) => {
        const o: DetourTurnStep = {};
        if (row.instruction_he.trim()) o.instruction_he = row.instruction_he.trim();
        if (row.instruction_en.trim()) o.instruction_en = row.instruction_en.trim();
        return Object.keys(o).length ? o : null;
      })
      .filter((x): x is DetourTurnStep => x != null);
    const narrative = instructionsTextHe.trim();
    if (!narrative && !road && turn_by_turn.length === 0) {
      setMessage("Enter Hebrew turn-by-turn (main box), or optional rows / Advanced road geometry.");
      return;
    }
    setDetourLoading(true);
    setMessage(null);
    try {
      const body: Record<string, unknown> = {
        mode: "route",
        start_date: areaStartDate.trim(),
        start_time: areaStartTime.trim() || "04:00",
        end_date: areaEndDate.trim(),
        end_time: areaEndTime.trim() || "23:59",
        blockage_geojson: blockageGeojson,
        max_routes: 20,
        transfer_radius_m: 250,
        use_osm_detour: useOSMDetour,
        routing_engine: detourRoutingEngine,
        route_id: selectedRoute.route_id,
        direction_id: selectedDirectionId ?? undefined,
        remember_override: rememberStreetOverride,
      };
      if (road) body.detour_road_geojson = road;
      if (narrative) body.instructions_text_he = narrative;
      if (turn_by_turn.length) body.turn_by_turn = turn_by_turn;
      if (manualStopBefore.trim()) body.stop_before = manualStopBefore.trim();
      if (manualStopAfter.trim()) body.stop_after = manualStopAfter.trim();
      const res = await axios.post<DetourByAreaResponse>(`${API_BASE}/detours/by-area`, body, {
        timeout: 120000,
        headers: { "Content-Type": "application/json" },
      });
      const feedVersion = res.data.feed_version ?? "";
      if (res.data.mode !== "route") {
        setMessage("Unexpected response mode from server.");
        return;
      }
      const r = res.data.result;
      setDetourAreaResults(r ? [r] : []);
      const hasPathFeatures = (r?.detour_geojson?.features?.length ?? 0) > 0;
      const hasInstructionsOnly =
        Boolean(r?.instructions_only) && Boolean(r.turn_by_turn && r.turn_by_turn.length > 0);
      if (!r) {
        setMessage("No result.");
      } else if (r.error) {
        setMessage(r.error);
      } else if (hasInstructionsOnly) {
        setDetour(mapAreaResultToDetour(r, feedVersion));
        setMessage(null);
      } else if (hasPathFeatures) {
        setDetour(mapAreaResultToDetour(r, feedVersion));
        setMessage(null);
        requestAnimationFrame(() => mapRef.current?.fitToDetour());
      } else if (r.detour_geojson && !hasPathFeatures) {
        setMessage("Server returned an empty detour path. Check stop overrides, direction, or try again.");
      } else {
        setMessage("Unexpected response: no detour path and no instructions. Check the server or try again.");
      }
    } catch (err: unknown) {
      console.error(err);
      setMessage(axios.isAxiosError(err) ? (err.response?.data?.detail ?? err.message) : "Manual detour request failed.");
    } finally {
      setDetourLoading(false);
    }
  };

  const handlePrepareManualDetour = (areaRoute: AreaRouteResult, dr: DetourByAreaRouteResult | null) => {
    const info: RouteInfo = {
      route_id: areaRoute.route_id,
      route_short_name: areaRoute.route_short_name ?? undefined,
      route_long_name: areaRoute.route_long_name ?? undefined,
      agency_name: areaRoute.agency_name ?? undefined,
    };
    setSelectedRoute(info);
    setSelectedDirectionId(areaRoute.direction_id != null ? String(areaRoute.direction_id) : null);
    setDetourMode("route");
    setManualDetourOpen(true);
    if (dr?.stop_before) setManualStopBefore(dr.stop_before);
    if (dr?.stop_after) setManualStopAfter(dr.stop_after);
    loadRouteOnMap(areaRoute.route_id, areaRoute.direction_id ?? null);
  };

  const resultByRouteId = useMemo(() => {
    const m = new Map<string, DetourByAreaRouteResult>();
    (detourAreaResults ?? []).forEach((r) => {
      m.set(`${r.route_id}\t${r.direction_id ?? ""}`, r);
    });
    return m;
  }, [detourAreaResults]);

  const diagnosticsPanel = useMemo(() => {
    if (!detour) return null;
    const diagnostics = detour.diagnostics ?? {};
    const knownKeys = new Set([
      "distance_ratio",
      "time_ratio",
      "road_distance_m",
      "road_time_s",
      "gtfs_cost",
      "candidate_count",
    ]);
    const extras = Object.entries(diagnostics).filter(([k]) => !knownKeys.has(k));
    const metric = (key: string): number | null => {
      const raw = (diagnostics as Record<string, unknown>)[key];
      return typeof raw === "number" && Number.isFinite(raw) ? raw : null;
    };
    const formatMetric = (value: number | null, suffix = "", digits = 2): string =>
      value == null ? "—" : `${value.toFixed(digits)}${suffix}`;
    const formatUnknown = (value: unknown): string => {
      if (value == null) return "—";
      if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
        return String(value);
      }
      try {
        return JSON.stringify(value);
      } catch {
        return "[unserializable]";
      }
    };
    const confidencePct =
      typeof detour.confidence === "number" && Number.isFinite(detour.confidence)
        ? `${Math.max(0, Math.min(100, detour.confidence * 100)).toFixed(0)}%`
        : "—";
    return {
      strategyUsed: detour.strategy_used ?? "—",
      reasonCode: detour.reason_code ?? "—",
      confidencePct,
      distanceRatio: metric("distance_ratio"),
      timeRatio: metric("time_ratio"),
      roadDistanceM: metric("road_distance_m"),
      roadTimeS: metric("road_time_s"),
      gtfsCost: metric("gtfs_cost"),
      candidateCount: metric("candidate_count"),
      extras: extras.map(([k, v]) => ({ key: k, value: formatUnknown(v) })),
      formatMetric,
    };
  }, [detour]);

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
            <button
              type="button"
              onClick={() => mapRef.current?.editBlockagePolygon()}
              disabled={!blockageGeojson}
              title="Drag vertices to reshape; use Done editing when the Cancel button shows that label."
            >
              Edit polygon
            </button>
            <button type="button" onClick={() => mapRef.current?.clearBlockage()} title="Clear blockage">
              Clear
            </button>
            <button
              type="button"
              onClick={() => mapRef.current?.cancelDrawing()}
              title={
                drawMode === "direct_select"
                  ? "Leave vertex edit mode; polygon stays."
                  : drawMode === "draw_polygon"
                    ? "Stop drawing (discards in-progress polygon)."
                    : "Cancel drawing or return to map view."
              }
            >
              {drawMode === "direct_select" ? "Done editing" : "Cancel"}
            </button>
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
          <div className="radio-group" style={{ marginTop: 8 }}>
            <span className="rail-label-small" style={{ display: "block", marginBottom: 4 }}>
              GTFS graph routing
            </span>
            <label>
              <input
                type="radio"
                name="detour-routing-engine"
                checked={detourRoutingEngine === "astar"}
                onChange={() => setDetourRoutingEngine("astar")}
              />{" "}
              A* (default)
            </label>
            <label>
              <input
                type="radio"
                name="detour-routing-engine"
                checked={detourRoutingEngine === "dijkstra"}
                onChange={() => setDetourRoutingEngine("dijkstra")}
              />{" "}
              Dijkstra
            </label>
          </div>
          <button
            type="button"
            className="btn-compute"
            onClick={handleComputeDetour}
            disabled={detourLoading || !blockageGeojson || !isTimeWindowValid}
          >
            {detourLoading ? "Computing…" : "Compute detour"}
          </button>
          <button
            type="button"
            className="btn-time-preset"
            style={{ marginTop: 8, width: "100%" }}
            onClick={() => setManualDetourOpen((o) => !o)}
          >
            {manualDetourOpen ? "Hide manual street detour" : "Manual street detour (after compute)"}
          </button>
          {manualDetourOpen && (
            <div className="manual-detour-panel" style={{ marginTop: 10 }}>
              <label className="rail-label-small">Turn-by-turn (Hebrew — separate steps with commas)</label>
              <textarea
                value={instructionsTextHe}
                onChange={(e) => setInstructionsTextHe(e.target.value)}
                dir="rtl"
                placeholder="תמשיך ישר בבלפור, שמאלה בישראל בן ציון, …"
                rows={5}
                style={{ width: "100%", fontSize: 14 }}
              />
              <p className="hint" style={{ marginTop: 4 }}>
                Hebrew text is for passenger instructions only — it does not draw a line. Under Advanced, use
                &quot;Draw detour path on map&quot; (or paste GeoJSON) to show the detour on the map and enable
                Remember with blockage checks.
              </p>
              <label className="rail-label-small">Entry / exit stop IDs (optional, must match failed compute)</label>
              <input
                type="text"
                placeholder="stop_before"
                value={manualStopBefore}
                onChange={(e) => setManualStopBefore(e.target.value)}
                style={{ width: "100%", marginBottom: 4 }}
              />
              <input
                type="text"
                placeholder="stop_after"
                value={manualStopAfter}
                onChange={(e) => setManualStopAfter(e.target.value)}
                style={{ width: "100%", marginBottom: 8 }}
              />
              <button
                type="button"
                className="btn-time-preset"
                style={{ marginBottom: 8 }}
                onClick={() => setManualGeoAdvancedOpen((o) => !o)}
              >
                {manualGeoAdvancedOpen ? "Hide advanced (GeoJSON / per-step)" : "Advanced: road GeoJSON or per-step rows"}
              </button>
              {manualGeoAdvancedOpen && (
                <>
                  <div className="rail-buttons" style={{ marginBottom: 8, flexWrap: "wrap" }}>
                    <button
                      type="button"
                      onClick={() => mapRef.current?.startDrawDetourLine()}
                      title="Click the map to add vertices; double-click or Enter to finish"
                    >
                      Draw detour path on map
                    </button>
                    <button type="button" onClick={() => mapRef.current?.clearManualDetourLine()}>
                      Clear drawn path
                    </button>
                  </div>
                  <label className="rail-label-small">Road geometry (GeoJSON LineString, lon/lat) — optional paste</label>
                  <textarea
                    value={manualRoadGeoJsonText}
                    onChange={(e) => {
                      const v = e.target.value;
                      setManualRoadGeoJsonText(v);
                      const t = v.trim();
                      if (!t) {
                        setManualDetourDraftLine(null);
                        mapRef.current?.applyManualDetourLineToDraw(null);
                        return;
                      }
                      try {
                        const parsed = JSON.parse(t) as unknown;
                        if (
                          parsed &&
                          typeof parsed === "object" &&
                          (parsed as GeoJSON.LineString).type === "LineString"
                        ) {
                          const ls = parsed as GeoJSON.LineString;
                          if (ls.coordinates && ls.coordinates.length >= 2) {
                            setManualDetourDraftLine(ls);
                            mapRef.current?.applyManualDetourLineToDraw(ls);
                          }
                        }
                      } catch {
                        /* invalid while typing */
                      }
                    }}
                    placeholder='{"type":"LineString","coordinates":[[34.8,32.0],[34.81,32.01]]}'
                    rows={4}
                    style={{ width: "100%", fontFamily: "monospace", fontSize: 12 }}
                  />
                  <div style={{ marginTop: 8 }}>
                    <span className="rail-label-small">Optional per-step rows (Hebrew / English)</span>
                    {manualTurnRows.map((row, i) => (
                      <div key={i} style={{ display: "flex", gap: 6, marginTop: 4, flexDirection: "column" }}>
                        <input
                          type="text"
                          dir="rtl"
                          placeholder="הוראה בעברית"
                          value={row.instruction_he}
                          onChange={(e) => {
                            const next = [...manualTurnRows];
                            next[i] = { ...next[i], instruction_he: e.target.value };
                            setManualTurnRows(next);
                          }}
                        />
                        <input
                          type="text"
                          placeholder="English instruction"
                          value={row.instruction_en}
                          onChange={(e) => {
                            const next = [...manualTurnRows];
                            next[i] = { ...next[i], instruction_en: e.target.value };
                            setManualTurnRows(next);
                          }}
                        />
                      </div>
                    ))}
                    <button
                      type="button"
                      className="btn-time-preset"
                      style={{ marginTop: 6 }}
                      onClick={() =>
                        setManualTurnRows([...manualTurnRows, { instruction_he: "", instruction_en: "" }])
                      }
                    >
                      + Step
                    </button>
                  </div>
                </>
              )}
              <label style={{ display: "block", marginTop: 8 }}>
                <input
                  type="checkbox"
                  checked={rememberStreetOverride}
                  onChange={(e) => setRememberStreetOverride(e.target.checked)}
                />{" "}
                Remember for this blockage (server)
              </label>
              <button
                type="button"
                className="btn-compute"
                style={{ marginTop: 10, width: "100%" }}
                onClick={handleApplyManualStreetDetour}
                disabled={detourLoading || !blockageGeojson || !isTimeWindowValid || !selectedRoute}
              >
                Apply manual detour
              </button>
              <p className="hint" style={{ marginTop: 6 }}>
                Use “One route” mode. Text-only saves narrative for replay; drawn road (Advanced) is used for map fit and
                blockage-safe Remember.
              </p>
            </div>
          )}
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
            title="Raster options use GPU tuning for clarity; vector uses OpenFreeMap (crisp labels). GovMap needs VITE_GOVMAP_TILES or proxy env — see govmapBasemapEnv.ts."
          >
            <option value="osm">OSM standard (raster)</option>
            <option value="carto_light">Carto light (minimal raster)</option>
            <option
              value="govmap"
              disabled={!govmapBasemapAvailable}
              title={
                govmapBasemapAvailable
                  ? "Web Mercator raster tiles (GovMap)"
                  : "Set VITE_GOVMAP_TILES (XYZ URLs) or VITE_GOVMAP_USE_PROXY=1 and GOVMAP_TILE_UPSTREAM_TEMPLATE on the API"
              }
            >
              GovMap (raster){govmapBasemapAvailable ? "" : " — configure env"}
            </option>
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

        {feedCalendarNotice && <div className="rail-status rail-status-feed">{feedCalendarNotice}</div>}
        {message && <div className="rail-status">{message}</div>}
        {detour && diagnosticsPanel && (
          <section className="rail-section diagnostics-panel">
            <button
              type="button"
              className="btn-time-preset diagnostics-toggle"
              onClick={() => setShowDiagnostics((v) => !v)}
            >
              {showDiagnostics ? "Hide diagnostics" : "Show diagnostics"}
            </button>
            {showDiagnostics && (
              <div className="diagnostics-body">
                <div className="diagnostics-summary">
                  <span className="diag-chip">strategy: {diagnosticsPanel.strategyUsed}</span>
                  <span className="diag-chip">reason: {diagnosticsPanel.reasonCode}</span>
                  <span className="diag-chip">confidence: {diagnosticsPanel.confidencePct}</span>
                </div>
                <div className="diagnostics-grid">
                  <div>distance_ratio</div>
                  <div>{diagnosticsPanel.formatMetric(diagnosticsPanel.distanceRatio, "", 2)}</div>
                  <div>time_ratio</div>
                  <div>{diagnosticsPanel.formatMetric(diagnosticsPanel.timeRatio, "", 2)}</div>
                  <div>road_distance_m</div>
                  <div>{diagnosticsPanel.formatMetric(diagnosticsPanel.roadDistanceM, " m", 0)}</div>
                  <div>road_time_s</div>
                  <div>{diagnosticsPanel.formatMetric(diagnosticsPanel.roadTimeS, " s", 0)}</div>
                  <div>gtfs_cost</div>
                  <div>{diagnosticsPanel.formatMetric(diagnosticsPanel.gtfsCost, "", 1)}</div>
                  <div>candidate_count</div>
                  <div>{diagnosticsPanel.formatMetric(diagnosticsPanel.candidateCount, "", 0)}</div>
                </div>
                {diagnosticsPanel.extras.length > 0 && (
                  <div className="diagnostics-extra">
                    {diagnosticsPanel.extras.map((row) => (
                      <div className="diagnostics-extra-row" key={row.key}>
                        <span>{row.key}</span>
                        <span>{row.value}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </section>
        )}
        {detour?.turn_by_turn && detour.turn_by_turn.length > 0 && (
          <section className="rail-section">
            <h2 className="rail-heading">Turn-by-turn</h2>
            {detour.from_override && (
              <p className="hint" style={{ marginBottom: 6 }}>
                Saved street override
              </p>
            )}
            <ol className="turn-by-turn-list" style={{ paddingInlineStart: 20, fontSize: 13 }}>
              {detour.turn_by_turn.map((s, i) => (
                <li key={i} style={{ marginBottom: 6 }}>
                  {(s.instruction_he || "").trim() || (s.instruction_en || "").trim() || s.street || "—"}
                  {(s.instruction_he || "").trim() && (s.instruction_en || "").trim() ? (
                    <div style={{ opacity: 0.85, fontSize: 12 }}>{s.instruction_en}</div>
                  ) : null}
                </li>
              ))}
            </ol>
          </section>
        )}
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
          manualDraftDetourLine={manualDetourDraftLine}
          onManualDetourLineChange={handleManualDetourLineFromMap}
          pinPosition={pinPosition}
          selectedStopId={selectedStop?.stop_id ?? null}
          basemap={basemap}
          onStopClick={handleMapStopClick}
          onStopOpenInExplorer={handleOpenStopInExplorer}
          onDrawModeChange={setDrawMode}
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
            onPrepareManualDetour={handlePrepareManualDetour}
          />
        )}
      </div>
    </div>
  );
};

export default App;

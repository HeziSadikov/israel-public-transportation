import React, { useEffect, useMemo, useState } from "react";
import { Rnd } from "react-rnd";

export type ExplorerTab = "area" | "line" | "point" | "address" | "stop";

export type RouteInfo = {
  route_id: string;
  route_short_name?: string;
  route_long_name?: string;
  agency_id?: string;
  agency_name?: string;
  trip_count?: number | null;
  last_stop_name?: string | null;
};

export type AreaRouteResult = {
  route_id: string;
  route_short_name?: string;
  route_long_name?: string;
  direction_id?: string | null;
  agency_id?: string | null;
  agency_name?: string | null;
  first_time?: string | null;
  last_time?: string | null;
  trip_count?: number | null;
  last_stop_name?: string | null;
};

export type ExplorerSortBy = "line_asc" | "line_desc" | "agency_asc" | "agency_desc" | "trips_desc" | "destination_asc";
type SortDir = "asc" | "desc";
type SortTable = "area" | "line" | "stop";
type SortColumn = "line" | "agency" | "trips" | "destination" | "route_pair" | "detour" | "time_window";

export type DetourByAreaRouteResult = {
  route_id: string;
  direction_id?: string | null;
  error?: string | null;
  detour_geojson?: unknown;
  detour_stop_path: string[];
  used_transfers: boolean;
  stop_before?: string | null;
  stop_after?: string | null;
  turn_by_turn?: { instruction_he?: string | null; instruction_en?: string | null; street?: string | null }[];
  from_override?: boolean;
  instructions_only?: boolean;
  reason_code?: string | null;
  strategy_used?: string | null;
};

export type GeocodeResult = {
  display_name: string;
  lat: number;
  lon: number;
  place_id?: number;
};

export type SelectedStopInfo = {
  stop_id: string;
  stop_name?: string;
  stop_code?: string | null;
  lat?: number;
  lon?: number;
};

export type StopLineResult = {
  route_id: string;
  direction_id?: string | null;
  route_short_name?: string | null;
  route_long_name?: string | null;
  agency_name?: string | null;
  first_time?: string | null;
  last_time?: string | null;
};

type ExplorerWindowProps = {
  activeTab: ExplorerTab;
  onTabChange: (tab: ExplorerTab) => void;
  isOpen: boolean;
  onMinimize: () => void;
  onClose: () => void;
  position: { x: number; y: number };
  size: { width: number; height: number };
  onPositionChange: (pos: { x: number; y: number }) => void;
  onSizeChange: (size: { width: number; height: number }) => void;
  // Area tab
  areaRoutes: AreaRouteResult[] | null;
  areaLoading: boolean;
  timeWindowValid: boolean;
  resultByRouteId: Map<string, DetourByAreaRouteResult>;
  selectedRouteId: string | null;
  onFindAreaRoutes: () => void;
  onSelectAreaRoute: (r: AreaRouteResult) => void;
  onFitToRoute: () => void;
  onUseForDetour: (r: AreaRouteResult) => void;
  hasBlockage: boolean;
  // Line tab
  lineSearchQuery: string;
  onLineSearchQueryChange: (q: string) => void;
  lineSearchResults: RouteInfo[];
  lineSearchLoading: boolean;
  onLineSearch: () => void;
  onSelectLineRoute: (r: RouteInfo) => void;
  // Point tab
  onGoToLatLng: (lat: number, lng: number) => void;
  // Address tab
  addressQuery: string;
  onAddressQueryChange: (q: string) => void;
  addressResults: GeocodeResult[];
  addressLoading: boolean;
  onAddressSearch: () => void;
  onSelectAddressResult: (r: GeocodeResult) => void;
  // Stop tab
  stopSearchQuery: string;
  onStopSearchQueryChange: (q: string) => void;
  stopSearchResults: SelectedStopInfo[];
  stopSearchLoading: boolean;
  stopSearchError: string | null;
  onSelectStopResult: (s: SelectedStopInfo) => void;
  selectedStop: SelectedStopInfo | null;
  stopLinesLoading: boolean;
  stopLinesResults: StopLineResult[];
  stopLinesError: string | null;
  stopLinesHint: string | null;
  onSearchLinesInStop: () => void;
  onSelectStopLineRoute: (r: StopLineResult) => void;
  sortBy: ExplorerSortBy;
  onPrepareManualDetour?: (areaRoute: AreaRouteResult, detourResult: DetourByAreaRouteResult | null) => void;
};

function getRouteResultStatus(r: DetourByAreaRouteResult): "detour" | "no-detour" | "error" {
  if (r.error) return "error";
  if (r.instructions_only && (r.turn_by_turn?.length ?? 0) > 0) return "detour";
  if (r.detour_geojson && Array.isArray(r.detour_stop_path) && r.detour_stop_path.length > 0) return "detour";
  return "no-detour";
}

/** Shown in Destination column: last stop name, else route long name. */
export function destinationLabel(r: {
  last_stop_name?: string | null;
  route_long_name?: string | null;
}): string {
  const t = (r.last_stop_name ?? "").trim() || (r.route_long_name ?? "").trim();
  return t || "—";
}

function destinationSortKey(r: {
  last_stop_name?: string | null;
  route_long_name?: string | null;
}): string {
  return (r.last_stop_name ?? "").trim() || (r.route_long_name ?? "").trim();
}

const TAB_LABELS: Record<ExplorerTab, string> = {
  area: "Area",
  line: "Line",
  point: "Point",
  address: "Address",
  stop: "Stop",
};

export const ExplorerWindow: React.FC<ExplorerWindowProps> = (props) => {
  const [pointLat, setPointLat] = useState("");
  const [pointLng, setPointLng] = useState("");
  const [sortByTable, setSortByTable] = useState<Record<SortTable, { column: SortColumn; dir: SortDir }>>({
    area: { column: "line", dir: "asc" },
    line: { column: "line", dir: "asc" },
    stop: { column: "line", dir: "asc" },
  });
  const [sortMemory, setSortMemory] = useState<Record<SortTable, Partial<Record<SortColumn, SortDir>>>>({
    area: { line: "asc", agency: "asc", trips: "desc", destination: "asc", route_pair: "asc", detour: "asc" },
    line: { line: "asc", agency: "asc", trips: "desc", destination: "asc", route_pair: "asc" },
    stop: { line: "asc", agency: "asc", destination: "asc", time_window: "asc" },
  });

  if (!props.isOpen) return null;

  useEffect(() => {
    const fromLegacySort = (): { column: SortColumn; dir: SortDir } => {
      switch (props.sortBy) {
        case "line_desc":
          return { column: "line", dir: "desc" };
        case "agency_asc":
          return { column: "agency", dir: "asc" };
        case "agency_desc":
          return { column: "agency", dir: "desc" };
        case "trips_desc":
          return { column: "trips", dir: "desc" };
        case "destination_asc":
          return { column: "destination", dir: "asc" };
        case "line_asc":
        default:
          return { column: "line", dir: "asc" };
      }
    };
    const s = fromLegacySort();
    setSortByTable((prev) => ({ ...prev, area: s, line: s }));
  }, [props.sortBy]);

  const handleGoToPoint = () => {
    const lat = parseFloat(pointLat.replace(",", "."));
    const lng = parseFloat(pointLng.replace(",", "."));
    if (Number.isFinite(lat) && Number.isFinite(lng)) {
      props.onGoToLatLng(lat, lng);
    }
  };

  const handleSortHeaderClick = (table: SortTable, column: SortColumn, defaultDir: SortDir = "asc") => {
    setSortByTable((prev) => {
      const active = prev[table];
      let nextDir: SortDir;
      if (active.column === column) {
        nextDir = active.dir === "asc" ? "desc" : "asc";
      } else {
        nextDir = sortMemory[table][column] ?? defaultDir;
      }
      setSortMemory((mem) => ({ ...mem, [table]: { ...mem[table], [column]: nextDir } }));
      return { ...prev, [table]: { column, dir: nextDir } };
    });
  };

  const sortArrow = (table: SortTable, column: SortColumn): string =>
    sortByTable[table].column === column ? (sortByTable[table].dir === "asc" ? "▲" : "▼") : "";

  const sortedAreaRoutes = useMemo(() => {
    const rows = [...(props.areaRoutes ?? [])];
    const cmpText = (a: string | null | undefined, b: string | null | undefined) =>
      (a ?? "").localeCompare(b ?? "", "he", { sensitivity: "base", numeric: true });
    const cmpLine = (a: string | null | undefined, b: string | null | undefined) =>
      (a ?? "").localeCompare(b ?? "", "he", { sensitivity: "base", numeric: true });
    const detourRank = (r: AreaRouteResult): number => {
      const key = `${r.route_id}\t${r.direction_id ?? ""}`;
      const result = props.resultByRouteId.get(key);
      if (!result) return 9;
      const status = getRouteResultStatus(result);
      if (status === "detour") return 1;
      if (status === "no-detour") return 2;
      return 3;
    };
    rows.sort((a, b) => {
      const { column, dir } = sortByTable.area;
      const s = dir === "asc" ? 1 : -1;
      switch (column) {
        case "agency":
          return s * cmpText(a.agency_name, b.agency_name) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "trips":
          return s * (Number(a.trip_count ?? 0) - Number(b.trip_count ?? 0)) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "destination":
          return s * cmpText(destinationSortKey(a), destinationSortKey(b)) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "route_pair":
          return s * cmpText(a.route_long_name, b.route_long_name) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "detour":
          return s * (detourRank(a) - detourRank(b)) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "line":
        default:
          return s * cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
      }
    });
    return rows;
  }, [props.areaRoutes, props.resultByRouteId, sortByTable.area]);

  const sortedLineRoutes = useMemo(() => {
    const rows = [...props.lineSearchResults];
    const cmpText = (a: string | null | undefined, b: string | null | undefined) =>
      (a ?? "").localeCompare(b ?? "", "he", { sensitivity: "base", numeric: true });
    const cmpLine = (a: string | null | undefined, b: string | null | undefined) =>
      (a ?? "").localeCompare(b ?? "", "he", { sensitivity: "base", numeric: true });
    rows.sort((a, b) => {
      const { column, dir } = sortByTable.line;
      const s = dir === "asc" ? 1 : -1;
      switch (column) {
        case "agency":
          return s * cmpText(a.agency_name, b.agency_name) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "trips":
          return s * (Number(a.trip_count ?? 0) - Number(b.trip_count ?? 0)) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "destination":
          return s * cmpText(destinationSortKey(a), destinationSortKey(b)) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "route_pair":
          return s * cmpText(a.route_long_name, b.route_long_name) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "line":
        default:
          return s * cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
      }
    });
    return rows;
  }, [props.lineSearchResults, sortByTable.line]);

  const sortedStopLineRoutes = useMemo(() => {
    const rows = [...props.stopLinesResults];
    const cmpText = (a: string | null | undefined, b: string | null | undefined) =>
      (a ?? "").localeCompare(b ?? "", "he", { sensitivity: "base", numeric: true });
    const cmpLine = (a: string | null | undefined, b: string | null | undefined) =>
      (a ?? "").localeCompare(b ?? "", "he", { sensitivity: "base", numeric: true });
    rows.sort((a, b) => {
      const { column, dir } = sortByTable.stop;
      const s = dir === "asc" ? 1 : -1;
      switch (column) {
        case "agency":
          return s * cmpText(a.agency_name, b.agency_name) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "destination":
          return s * cmpText(destinationLabel({ route_long_name: a.route_long_name }), destinationLabel({ route_long_name: b.route_long_name })) ||
            cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "time_window":
          return s * cmpText(`${a.first_time ?? ""}-${a.last_time ?? ""}`, `${b.first_time ?? ""}-${b.last_time ?? ""}`) ||
            cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "line":
        default:
          return s * cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
      }
    });
    return rows;
  }, [props.stopLinesResults, sortByTable.stop]);

  const content = (
    <div className="explorer-content">
      {props.activeTab === "area" && (
        <div className="explorer-tab-area">
          <button
            type="button"
            className="btn-find-area"
            onClick={props.onFindAreaRoutes}
            disabled={!props.hasBlockage || props.areaLoading || !props.timeWindowValid}
          >
            {props.areaLoading ? "Searching…" : "Find lines in polygon"}
          </button>
          {props.areaRoutes && props.areaRoutes.length > 0 ? (
            <div className="lines-table-wrap">
              <p className="lines-table-scroll-hint">Scroll horizontally to see all columns.</p>
              <div className="explorer-results-scroll">
                <table className="lines-table" dir="rtl">
                  <thead>
                    <tr>
                      <th className="col-index">#</th>
                      <th className="col-num sortable" onClick={() => handleSortHeaderClick("area", "line", "asc")}>line {sortArrow("area", "line")}</th>
                      <th className="col-operator sortable" onClick={() => handleSortHeaderClick("area", "agency", "asc")}>agency {sortArrow("area", "agency")}</th>
                      <th className="col-trips sortable" onClick={() => handleSortHeaderClick("area", "trips", "desc")}>trips {sortArrow("area", "trips")}</th>
                      <th className="col-dest sortable" onClick={() => handleSortHeaderClick("area", "destination", "asc")}>destination {sortArrow("area", "destination")}</th>
                      <th className="col-route-pair sortable" onClick={() => handleSortHeaderClick("area", "route_pair", "asc")}>location &lt;-&gt; destination {sortArrow("area", "route_pair")}</th>
                      <th className="col-status sortable" onClick={() => handleSortHeaderClick("area", "detour", "asc")}>detour {sortArrow("area", "detour")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedAreaRoutes.slice(0, 100).map((r, i) => {
                      const key = `${r.route_id}\t${r.direction_id ?? ""}`;
                      const result = props.resultByRouteId.get(key);
                      const status = result ? getRouteResultStatus(result) : null;
                      const isSelected = props.selectedRouteId === r.route_id;
                      return (
                        <tr
                          key={key}
                          className={`lines-table-row ${isSelected ? "selected" : ""}`}
                          onClick={() => props.onSelectAreaRoute(r)}
                          onDoubleClick={() => props.onFitToRoute()}
                        >
                          <td className="col-index">{i + 1}</td>
                          <td className="col-num">{r.route_short_name ?? r.route_id}</td>
                          <td className="col-operator" title={r.agency_name ?? ""}>{r.agency_name ?? "—"}</td>
                          <td className="col-trips" title={r.first_time && r.last_time ? `${r.first_time}-${r.last_time}` : "Trip count in selected time window"}>
                            {r.trip_count ?? 0}
                          </td>
                          <td className="col-dest" title={destinationLabel(r)}>{destinationLabel(r)}</td>
                          <td className="col-route-pair" title={r.route_long_name ?? ""}>{r.route_long_name ?? "—"}</td>
                          <td className="col-status">
                            {status ? (
                              <span className={`badge badge-${status}`}>
                                {status === "detour" &&
                                  (result?.strategy_used === "gtfs_road_hybrid"
                                    ? "Detour (hybrid)"
                                    : result?.strategy_used === "gtfs_multiroute" && result?.reason_code === "gtfs_only_fallback"
                                      ? "Detour (GTFS fallback)"
                                      : result?.instructions_only
                                    ? "Detour (instructions)"
                                    : result?.used_transfers
                                      ? "Detour (transfers)"
                                      : "Detour")}
                                {status === "no-detour" && "No detour"}
                                {status === "error" && (result?.error ?? "Error")}
                              </span>
                            ) : "—"}
                            {status === "error" && props.onPrepareManualDetour ? (
                              <button
                                type="button"
                                className="btn-time-preset"
                                style={{ marginTop: 4, display: "block" }}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  props.onPrepareManualDetour!(r, result ?? null);
                                }}
                              >
                                Manual detour…
                              </button>
                            ) : null}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              {sortedAreaRoutes.length > 100 && (
                <p className="hint">Showing first 100 of {sortedAreaRoutes.length}</p>
              )}
            </div>
          ) : (
            <p className="hint">Draw a blockage, then click &quot;Find lines in polygon&quot;.</p>
          )}
        </div>
      )}

      {props.activeTab === "line" && (
        <div className="explorer-tab-line">
          <div className="row">
            <input
              type="text"
              value={props.lineSearchQuery}
              onChange={(e) => props.onLineSearchQueryChange(e.target.value)}
              placeholder="Line number or route name…"
              onKeyDown={(e) => e.key === "Enter" && props.onLineSearch()}
            />
            <button type="button" onClick={props.onLineSearch} disabled={props.lineSearchLoading || !props.timeWindowValid}>
              {props.lineSearchLoading ? "Searching…" : "Search"}
            </button>
          </div>
          <div className="lines-table-wrap">
            <h3 className="lines-table-title">טבלת קווים</h3>
            <p className="lines-table-hint">הקלק על שורה להצגת קו</p>
            <p className="lines-table-scroll-hint">Scroll horizontally to see all columns.</p>
            <div className="explorer-results-scroll">
              <table className="lines-table" dir="rtl">
                <thead>
                  <tr>
                    <th className="col-index">#</th>
                    <th className="col-num sortable" onClick={() => handleSortHeaderClick("line", "line", "asc")}>line {sortArrow("line", "line")}</th>
                    <th className="col-operator sortable" onClick={() => handleSortHeaderClick("line", "agency", "asc")}>agency {sortArrow("line", "agency")}</th>
                    <th className="col-trips sortable" onClick={() => handleSortHeaderClick("line", "trips", "desc")}>trips {sortArrow("line", "trips")}</th>
                    <th className="col-dest sortable" onClick={() => handleSortHeaderClick("line", "destination", "asc")}>destination {sortArrow("line", "destination")}</th>
                    <th className="col-route-pair sortable" onClick={() => handleSortHeaderClick("line", "route_pair", "asc")}>location &lt;-&gt; destination {sortArrow("line", "route_pair")}</th>
                  </tr>
                </thead>
                <tbody>
                  {sortedLineRoutes.map((r, i) => (
                    <tr
                      key={r.route_id}
                      className="lines-table-row"
                      onClick={() => props.onSelectLineRoute(r)}
                    >
                      <td className="col-index">{i + 1}</td>
                      <td className="col-num">{r.route_short_name ?? r.route_id}</td>
                      <td className="col-operator" title={r.agency_name ?? ""}>{r.agency_name ?? "—"}</td>
                      <td className="col-trips">{r.trip_count ?? 0}</td>
                      <td className="col-dest" title={destinationLabel(r)}>{destinationLabel(r)}</td>
                      <td className="col-route-pair" title={r.route_long_name ?? ""}>{r.route_long_name ?? "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {props.activeTab === "point" && (
        <div className="explorer-tab-point">
          <label>Latitude</label>
          <input
            type="text"
            value={pointLat}
            onChange={(e) => setPointLat(e.target.value)}
            placeholder="e.g. 31.77"
          />
          <label>Longitude</label>
          <input
            type="text"
            value={pointLng}
            onChange={(e) => setPointLng(e.target.value)}
            placeholder="e.g. 35.21"
          />
          <button type="button" onClick={handleGoToPoint}>
            Go
          </button>
        </div>
      )}

      {props.activeTab === "address" && (
        <div className="explorer-tab-address">
          <div className="row">
            <input
              type="text"
              value={props.addressQuery}
              onChange={(e) => props.onAddressQueryChange(e.target.value)}
              placeholder="Address or place…"
              onKeyDown={(e) => e.key === "Enter" && props.onAddressSearch()}
            />
            <button
              type="button"
              onClick={props.onAddressSearch}
              disabled={props.addressLoading}
            >
              {props.addressLoading ? "Searching…" : "Search"}
            </button>
          </div>
          <div className="explorer-address-list">
            {props.addressResults.map((r, i) => (
              <div
                key={r.place_id ?? i}
                className="explorer-address-row"
                onClick={() => props.onSelectAddressResult(r)}
              >
                {r.display_name}
              </div>
            ))}
          </div>
        </div>
      )}

      {props.activeTab === "stop" && (
        <div className="explorer-tab-stop">
          <div className="row">
            <input
              type="text"
              value={props.stopSearchQuery}
              onChange={(e) => props.onStopSearchQueryChange(e.target.value)}
              placeholder="Search stop by name, code, or ID…"
            />
          </div>
          {props.stopSearchLoading && <p className="hint">Searching stops…</p>}
          {!props.stopSearchLoading && props.stopSearchError && <p className="hint">{props.stopSearchError}</p>}
          <div className="explorer-stop-list">
            {props.stopSearchResults.length > 0 ? (
              props.stopSearchResults.slice(0, 50).map((s) => (
                <div
                  key={`${s.stop_id}:${s.lat ?? ""}:${s.lon ?? ""}`}
                  className={`explorer-stop-row ${
                    props.selectedStop?.stop_id === s.stop_id ? "selected" : ""
                  }`}
                  onClick={() => props.onSelectStopResult(s)}
                >
                  <div className="explorer-stop-main">{s.stop_name || "Unnamed stop"}</div>
                  <div className="explorer-stop-meta">
                    <span>Code: {s.stop_code || "N/A"}</span>
                    <span>ID: {s.stop_id}</span>
                  </div>
                </div>
              ))
            ) : props.stopSearchQuery.trim().length >= 2 && !props.stopSearchLoading ? (
              <p className="hint">No matching stops.</p>
            ) : (
              <p className="hint">
                Type at least 2 characters to search by stop name, code, or ID.
              </p>
            )}
          </div>
          <div className="explorer-stop-selected-card">
            <h3>Selected stop</h3>
            {props.selectedStop ? (
              <>
                <p>
                  <strong>Name:</strong> {props.selectedStop.stop_name || "Unnamed stop"}
                </p>
                <p>
                  <strong>Stop number (code):</strong> {props.selectedStop.stop_code || "N/A"}
                </p>
                <p>
                  <strong>Stop ID:</strong> {props.selectedStop.stop_id}
                </p>
              </>
            ) : (
              <p className="hint">Click a stop on the map or choose one from the list.</p>
            )}
            <button
              type="button"
              className="btn-stop-lines"
              onClick={props.onSearchLinesInStop}
              disabled={!props.selectedStop || props.stopLinesLoading || !props.timeWindowValid}
            >
              {props.stopLinesLoading ? "Searching…" : "Search lines in stop"}
            </button>
          </div>
          {props.stopLinesHint && <p className="hint">{props.stopLinesHint}</p>}
          {props.stopLinesError && <p className="hint">{props.stopLinesError}</p>}
          <div className="lines-table-wrap">
            {props.stopLinesResults.length > 0 ? (
              <>
                <p className="lines-table-scroll-hint">Scroll horizontally to see all columns.</p>
                <div className="explorer-results-scroll">
                  <table className="lines-table" dir="rtl">
                    <thead>
                      <tr>
                        <th className="col-index">#</th>
                        <th className="col-num sortable" onClick={() => handleSortHeaderClick("stop", "line", "asc")}>line {sortArrow("stop", "line")}</th>
                        <th className="col-operator sortable" onClick={() => handleSortHeaderClick("stop", "agency", "asc")}>agency {sortArrow("stop", "agency")}</th>
                        <th className="col-dest sortable" onClick={() => handleSortHeaderClick("stop", "destination", "asc")}>destination {sortArrow("stop", "destination")}</th>
                        <th className="col-route-pair sortable" onClick={() => handleSortHeaderClick("stop", "time_window", "asc")}>time window {sortArrow("stop", "time_window")}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortedStopLineRoutes.map((r, i) => (
                        <tr
                          key={`${r.route_id}:${r.direction_id ?? ""}:${i}`}
                          className="lines-table-row"
                          onClick={() => props.onSelectStopLineRoute(r)}
                        >
                          <td className="col-index">{i + 1}</td>
                          <td className="col-num">{r.route_short_name ?? r.route_id}</td>
                          <td className="col-operator">{r.agency_name ?? "—"}</td>
                          <td className="col-dest">{destinationLabel({ route_long_name: r.route_long_name })}</td>
                          <td className="col-route-pair">{r.first_time && r.last_time ? `${r.first_time}-${r.last_time}` : "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            ) : (
              <p className="hint">No lines loaded for this stop yet.</p>
            )}
          </div>
        </div>
      )}
    </div>
  );

  const header = (
    <div className="explorer-header">
      <div className="explorer-tabs">
        {(Object.keys(TAB_LABELS) as ExplorerTab[]).map((tab) => (
          <button
            key={tab}
            type="button"
            className={`explorer-tab-btn ${props.activeTab === tab ? "active" : ""}`}
            onClick={() => props.onTabChange(tab)}
          >
            {TAB_LABELS[tab]}
          </button>
        ))}
      </div>
      <div className="explorer-header-actions" onPointerDown={(e) => e.stopPropagation()}>
        <button type="button" className="btn-icon" onClick={props.onMinimize} title="Hide explorer">
          −
        </button>
        <button type="button" className="btn-icon" onClick={props.onClose} title="Close">
          ×
        </button>
      </div>
    </div>
  );

  return (
    <Rnd
      className="explorer-rnd"
      size={{ width: props.size.width, height: props.size.height }}
      position={{ x: props.position.x, y: props.position.y }}
      onDragStop={(_e, d) => props.onPositionChange({ x: d.x, y: d.y })}
      onResizeStop={(_e, _dir, ref, _delta, pos) => {
        props.onSizeChange({ width: ref.offsetWidth, height: ref.offsetHeight });
        props.onPositionChange({ x: pos.x, y: pos.y });
      }}
      minWidth={560}
      minHeight={320}
      maxWidth={1000}
      maxHeight={80 * (typeof window !== "undefined" ? window.innerHeight / 100 : 80)}
      dragHandleClassName="explorer-header"
      bounds="parent"
      enableResizing
    >
      <div className="explorer-window">
        {header}
        {content}
      </div>
    </Rnd>
  );
};

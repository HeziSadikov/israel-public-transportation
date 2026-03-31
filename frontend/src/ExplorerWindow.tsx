import React, { useMemo, useState } from "react";
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

export type DetourByAreaRouteResult = {
  route_id: string;
  direction_id?: string | null;
  error?: string | null;
  detour_geojson?: unknown;
  detour_stop_path: string[];
  used_transfers: boolean;
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
};

function getRouteResultStatus(r: DetourByAreaRouteResult): "detour" | "no-detour" | "error" {
  if (r.error) return "error";
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

  if (!props.isOpen) return null;

  const handleGoToPoint = () => {
    const lat = parseFloat(pointLat.replace(",", "."));
    const lng = parseFloat(pointLng.replace(",", "."));
    if (Number.isFinite(lat) && Number.isFinite(lng)) {
      props.onGoToLatLng(lat, lng);
    }
  };

  const sortedAreaRoutes = useMemo(() => {
    const rows = [...(props.areaRoutes ?? [])];
    const cmpText = (a: string | null | undefined, b: string | null | undefined) =>
      (a ?? "").localeCompare(b ?? "", "he", { sensitivity: "base", numeric: true });
    const cmpLine = (a: string | null | undefined, b: string | null | undefined) =>
      (a ?? "").localeCompare(b ?? "", "he", { sensitivity: "base", numeric: true });
    rows.sort((a, b) => {
      switch (props.sortBy) {
        case "line_desc":
          return cmpLine(b.route_short_name ?? b.route_id, a.route_short_name ?? a.route_id);
        case "agency_asc":
          return cmpText(a.agency_name, b.agency_name) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "agency_desc":
          return cmpText(b.agency_name, a.agency_name) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "trips_desc":
          return Number(b.trip_count ?? 0) - Number(a.trip_count ?? 0) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "destination_asc":
          return (
            cmpText(destinationSortKey(a), destinationSortKey(b)) ||
            cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id)
          );
        case "line_asc":
        default:
          return cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
      }
    });
    return rows;
  }, [props.areaRoutes, props.sortBy]);

  const sortedLineRoutes = useMemo(() => {
    const rows = [...props.lineSearchResults];
    const cmpText = (a: string | null | undefined, b: string | null | undefined) =>
      (a ?? "").localeCompare(b ?? "", "he", { sensitivity: "base", numeric: true });
    const cmpLine = (a: string | null | undefined, b: string | null | undefined) =>
      (a ?? "").localeCompare(b ?? "", "he", { sensitivity: "base", numeric: true });
    rows.sort((a, b) => {
      switch (props.sortBy) {
        case "line_desc":
          return cmpLine(b.route_short_name ?? b.route_id, a.route_short_name ?? a.route_id);
        case "agency_asc":
          return cmpText(a.agency_name, b.agency_name) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "agency_desc":
          return cmpText(b.agency_name, a.agency_name) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "trips_desc":
          return Number(b.trip_count ?? 0) - Number(a.trip_count ?? 0) || cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
        case "destination_asc":
          return (
            cmpText(destinationSortKey(a), destinationSortKey(b)) ||
            cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id)
          );
        case "line_asc":
        default:
          return cmpLine(a.route_short_name ?? a.route_id, b.route_short_name ?? b.route_id);
      }
    });
    return rows;
  }, [props.lineSearchResults, props.sortBy]);

  const content = (
    <div className="explorer-content">
      {props.activeTab === "area" && (
        <div className="explorer-tab-area">
          <button
            type="button"
            className="btn-find-area"
            onClick={props.onFindAreaRoutes}
            disabled={!props.hasBlockage || props.areaLoading}
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
                      <th className="col-num">line number</th>
                      <th className="col-operator">agency</th>
                      <th className="col-trips">number of trips</th>
                      <th className="col-dest">destination</th>
                      <th className="col-route-pair">location &lt;-&gt; destination</th>
                      <th className="col-status">status</th>
                      <th className="col-actions">actions</th>
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
                                {status === "detour" && (result?.used_transfers ? "Detour (transfers)" : "Detour")}
                                {status === "no-detour" && "No detour"}
                                {status === "error" && (result?.error ?? "Error")}
                              </span>
                            ) : "—"}
                          </td>
                          <td className="col-actions">
                            <button
                              type="button"
                              className="btn-use-detour"
                              onClick={(e) => {
                                e.stopPropagation();
                                props.onUseForDetour(r);
                              }}
                            >
                              Use
                            </button>
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
            <button type="button" onClick={props.onLineSearch} disabled={props.lineSearchLoading}>
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
                    <th className="col-num">line number</th>
                    <th className="col-operator">agency</th>
                    <th className="col-trips">number of trips</th>
                    <th className="col-dest">destination</th>
                    <th className="col-route-pair">location &lt;-&gt; destination</th>
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
              disabled={!props.selectedStop || props.stopLinesLoading}
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
                        <th className="col-num">line number</th>
                        <th className="col-operator">agency</th>
                        <th className="col-dest">destination</th>
                        <th className="col-route-pair">time window</th>
                      </tr>
                    </thead>
                    <tbody>
                      {props.stopLinesResults.map((r, i) => (
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
      minWidth={280}
      minHeight={200}
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

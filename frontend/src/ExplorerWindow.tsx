import React, { useState } from "react";
import { Rnd } from "react-rnd";

export type ExplorerTab = "area" | "line" | "point" | "address";

export type RouteInfo = {
  route_id: string;
  route_short_name?: string;
  route_long_name?: string;
  agency_id?: string;
  agency_name?: string;
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
};

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

type ExplorerWindowProps = {
  activeTab: ExplorerTab;
  onTabChange: (tab: ExplorerTab) => void;
  isOpen: boolean;
  isMinimized: boolean;
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
};

function getRouteResultStatus(r: DetourByAreaRouteResult): "detour" | "no-detour" | "error" {
  if (r.error) return "error";
  if (r.detour_geojson && Array.isArray(r.detour_stop_path) && r.detour_stop_path.length > 0) return "detour";
  return "no-detour";
}

const TAB_LABELS: Record<ExplorerTab, string> = {
  area: "Area",
  line: "Line",
  point: "Point",
  address: "Address",
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
            <div className="explorer-area-list">
              {props.areaRoutes.slice(0, 100).map((r) => {
                const key = `${r.route_id}\t${r.direction_id ?? ""}`;
                const result = props.resultByRouteId.get(key);
                const status = result ? getRouteResultStatus(result) : null;
                const isSelected = props.selectedRouteId === r.route_id;
                return (
                  <div
                    key={key}
                    className={`explorer-area-row ${isSelected ? "selected" : ""}`}
                    onClick={() => props.onSelectAreaRoute(r)}
                    onDoubleClick={() => props.onFitToRoute()}
                  >
                    <div className="explorer-area-main">
                      <strong>{r.route_short_name ?? r.route_id}</strong>
                      {r.direction_id != null && <span className="dir"> dir {r.direction_id}</span>}
                      {r.route_long_name && <span className="long">{r.route_long_name}</span>}
                    </div>
                    <div className="explorer-area-meta">
                      {r.agency_name && <span>{r.agency_name}</span>}
                      {r.first_time != null && r.last_time != null && (
                        <span>{r.first_time}–{r.last_time}</span>
                      )}
                    </div>
                    {status && (
                      <span className={`badge badge-${status}`}>
                        {status === "detour" && (result?.used_transfers ? "Detour (transfers)" : "Detour")}
                        {status === "no-detour" && "No detour"}
                        {status === "error" && (result?.error ?? "Error")}
                      </span>
                    )}
                    <button
                      type="button"
                      className="btn-use-detour"
                      onClick={(e) => {
                        e.stopPropagation();
                        props.onUseForDetour(r);
                      }}
                    >
                      Use for route-only detour
                    </button>
                  </div>
                );
              })}
              {props.areaRoutes.length > 100 && (
                <p className="hint">Showing first 100 of {props.areaRoutes.length}</p>
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
            <table className="lines-table" dir="rtl">
              <thead>
                <tr>
                  <th className="col-index">#</th>
                  <th className="col-num">קו</th>
                  <th className="col-code">מקט הקו</th>
                  <th className="col-id">מזהה</th>
                  <th className="col-operator">מפעיל</th>
                  <th className="col-trips">נס&apos;</th>
                  <th className="col-dest">יעד</th>
                  <th className="col-desc">תיאור הקו</th>
                </tr>
              </thead>
              <tbody>
                {props.lineSearchResults.map((r, i) => (
                  <tr
                    key={r.route_id}
                    className="lines-table-row"
                    onClick={() => props.onSelectLineRoute(r)}
                  >
                    <td className="col-index">{i}</td>
                    <td className="col-num">{r.route_short_name ?? r.route_id}</td>
                    <td className="col-code">{r.route_id}</td>
                    <td className="col-id">{r.route_id}</td>
                    <td className="col-operator">{r.agency_name ?? "—"}</td>
                    <td className="col-trips">—</td>
                    <td className="col-dest">{r.route_long_name ?? "—"}</td>
                    <td className="col-desc">{r.route_long_name ?? r.route_short_name ?? r.route_id}</td>
                  </tr>
                ))}
              </tbody>
            </table>
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
        <button type="button" className="btn-icon" onClick={props.onMinimize} title="Collapse">
          −
        </button>
        <button type="button" className="btn-icon" onClick={props.onClose} title="Close">
          ×
        </button>
      </div>
    </div>
  );

  const body = props.isMinimized ? null : content;

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
      maxWidth={600}
      maxHeight={80 * (typeof window !== "undefined" ? window.innerHeight / 100 : 80)}
      dragHandleClassName="explorer-header"
      bounds="parent"
      enableResizing={!props.isMinimized}
    >
      <div className="explorer-window">
        {header}
        {body}
      </div>
    </Rnd>
  );
};

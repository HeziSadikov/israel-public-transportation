import React, { useState } from "react";
import { postDetourComputeV2, postIncident } from "./api/detourV2";
import type { DetourV2ComputeResult, DetourV2Attempt } from "./api/detourV2";
import type { RouteInfo } from "./ExplorerWindow";

type Props = {
  blockageGeojson: GeoJSON.Geometry | null;
  startDateYmd: string;
  startTime: string;
  endDateYmd: string;
  endTime: string;
  selectedRoute: RouteInfo | null;
  onV2Computed?: (result: DetourV2ComputeResult | null) => void;
};

function AttemptsTable({ attempts }: { attempts: DetourV2Attempt[] }) {
  if (!attempts.length) return null;
  return (
    <table style={{ width: "100%", fontSize: 10, borderCollapse: "collapse", marginTop: 4 }}>
      <thead>
        <tr style={{ background: "var(--color-bg-alt, #f0f0f0)" }}>
          <th style={thStyle}>Anchor</th>
          <th style={thStyle}>Corridor</th>
          <th style={thStyle}>Candidates</th>
          <th style={thStyle}>Outcome / Error</th>
          <th style={thStyle}>ms</th>
        </tr>
      </thead>
      <tbody>
        {attempts.map((a, i) => (
          <tr key={i} style={{ borderBottom: "1px solid var(--color-border, #ddd)" }}>
            <td style={tdStyle}>{a.anchor_index ?? "-"}</td>
            <td style={tdStyle}>{a.corridor ?? "-"}</td>
            <td style={tdStyle}>{a.candidate_count ?? "-"}</td>
            <td style={tdStyle}>{a.error_type ?? a.reason ?? a.outcome ?? ""}</td>
            <td style={tdStyle}>{a.elapsed_ms ?? "-"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

const thStyle: React.CSSProperties = {
  padding: "2px 4px",
  textAlign: "left",
  fontWeight: 600,
  borderBottom: "1px solid var(--color-border, #ccc)",
};
const tdStyle: React.CSSProperties = { padding: "2px 4px" };

/**
 * Detour v2: create incident preview (affected routes + OSM edge bans) and compute road-level detours
 * for the selected route (representative trip) or an optional explicit trip id override.
 */
export function IncidentDetourV2Panel(props: Props) {
  const [tripId, setTripId] = useState("");
  const [incidentId, setIncidentId] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [lastResult, setLastResult] = useState<DetourV2ComputeResult | null>(null);
  const [showDiag, setShowDiag] = useState(false);
  const [debugDetour, setDebugDetour] = useState(false);
  const [useMatchedPhysical, setUseMatchedPhysical] = useState(false);

  const hasRouteOrTrip =
    !!props.selectedRoute?.route_id || tripId.trim().length > 0;
  const canUse = !!props.blockageGeojson && hasRouteOrTrip;

  const handleIncident = async () => {
    if (!props.blockageGeojson) {
      setMessage("Draw a blockage polygon first.");
      return;
    }
    setLoading(true);
    setMessage(null);
    try {
      const r = await postIncident({
        polygon_geojson: props.blockageGeojson,
        start_date: props.startDateYmd,
        start_time: props.startTime,
        end_date: props.endDateYmd,
        end_time: props.endTime,
      });
      setIncidentId(r.incident_id);
      setMessage(
        `Incident ${r.incident_id}: ~${r.affected_route_count} route rows, ${r.derived_edge_ban_count} OSM edge bans (policy ${r.policy_version}).`
      );
    } catch (e: unknown) {
      setMessage(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleCompute = async () => {
    if (!props.blockageGeojson) {
      setMessage("Draw a blockage polygon first.");
      return;
    }
    const override = tripId.trim();
    if (!override && !props.selectedRoute?.route_id) {
      setMessage("Select a route in the explorer or enter an optional trip id.");
      return;
    }
    setLoading(true);
    setMessage(null);
    try {
      const body =
        override.length > 0
          ? {
              service_date: props.startDateYmd,
              trip_ids: [override],
              blockage_geojson: props.blockageGeojson,
              incident_id: incidentId,
              persist: true as const,
              debug_detour: debugDetour,
              use_matched_physical: useMatchedPhysical,
            }
          : {
              service_date: props.startDateYmd,
              route_id: props.selectedRoute!.route_id,
              blockage_geojson: props.blockageGeojson,
              incident_id: incidentId,
              persist: true as const,
              debug_detour: debugDetour,
              use_matched_physical: useMatchedPhysical,
            };
      const r = await postDetourComputeV2(body);
      const first = (r.results?.[0] ?? null) as DetourV2ComputeResult | null;
      props.onV2Computed?.(first);
      setLastResult(first);
      const routeEcho =
        first?.route_id != null ? ` route_id=${first.route_id}.` : "";
      const statusNote = first?.selected?.summary_en
        ? ` ${first.selected.summary_en}`
        : "";
      setMessage(
        `Computed (policy ${r.policy_version}). Request ids: ${r.detour_request_ids.join(", ") || "none"}.${routeEcho}${statusNote}`
      );
    } catch (e: unknown) {
      setMessage(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const attempts = lastResult?.attempts ?? [];
  const skipped = lastResult?.stitching?.skipped_stop_ids ?? [];
  const served = lastResult?.stitching?.served_stop_ids ?? [];
  const breakdown = lastResult?.selected?.score_breakdown;

  return (
    <section className="rail-section time-window-section">
      <h2 className="rail-heading">Detour v2 (beta)</h2>
      <p className="rail-hint">
        Select a route in the explorer, then compute (uses the main pattern&apos;s trip for the service date). Optional:
        override with a GTFS <code>trip_id</code>. Requires PostGIS shape + optional Valhalla.
      </p>
      <label className="rail-label">
        Trip id (optional override)
        <input
          className="rail-input"
          type="text"
          value={tripId}
          onChange={(e) => setTripId(e.target.value)}
          placeholder="Leave empty to use selected route"
          spellCheck={false}
        />
      </label>
      <label className="rail-label" style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 6 }}>
        <input
          type="checkbox"
          checked={debugDetour}
          onChange={(e) => setDebugDetour(e.target.checked)}
        />
        <span>Debug GeoJSON (anchors, shape, blockage layers in API response)</span>
      </label>
      <label className="rail-label" style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <input
          type="checkbox"
          checked={useMatchedPhysical}
          onChange={(e) => setUseMatchedPhysical(e.target.checked)}
        />
        <span>Use matched physical geometry (when PostGIS backfill exists)</span>
      </label>
      <div className="rail-buttons">
        <button type="button" disabled={loading || !props.blockageGeojson} onClick={handleIncident}>
          Create incident (preview)
        </button>
        <button type="button" disabled={loading || !canUse} onClick={handleCompute}>
          Compute detour v2
        </button>
      </div>
      {message && <p className="rail-status">{message}</p>}

      {lastResult && (
        <div style={{ marginTop: 6 }}>
          <button
            type="button"
            style={{ fontSize: 11, padding: "2px 8px", marginBottom: 4 }}
            onClick={() => setShowDiag((v) => !v)}
          >
            {showDiag ? "▲ Hide diagnostics" : "▼ Show diagnostics"}
          </button>
          {showDiag && (
            <div style={{ border: "1px solid var(--color-border, #ddd)", borderRadius: 4, padding: 6, fontSize: 11 }}>
              {/* Status and corridor */}
              <div style={{ marginBottom: 4 }}>
                <strong>Status:</strong> {lastResult.status ?? "—"}{" "}
                {lastResult.corridor_stage && <span>| corridor: <em>{lastResult.corridor_stage}</em></span>}
              </div>

              {/* Selected summary */}
              {lastResult.selected?.summary_en && (
                <div style={{ marginBottom: 4, color: "var(--color-success, #2a8a2a)" }}>
                  <strong>Winner:</strong> {lastResult.selected.summary_en}
                </div>
              )}

              {/* Score breakdown */}
              {breakdown && (
                <details style={{ marginBottom: 4 }}>
                  <summary style={{ cursor: "pointer", fontWeight: 600 }}>Score breakdown</summary>
                  <table style={{ fontSize: 10, borderCollapse: "collapse", marginTop: 2 }}>
                    <tbody>
                      {Object.entries(breakdown).map(([k, v]) => (
                        <tr key={k}>
                          <td style={{ padding: "1px 6px 1px 0", color: "#666" }}>{k}</td>
                          <td style={{ padding: "1px 0" }}>{typeof v === "number" ? v.toFixed(1) : String(v ?? "—")}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </details>
              )}

              {/* Stops */}
              {(skipped.length > 0 || served.length > 0) && (
                <div style={{ marginBottom: 4 }}>
                  {skipped.length > 0 && (
                    <span style={{ color: "var(--color-warn, #b06000)", marginRight: 8 }}>
                      <strong>Skipped:</strong> {skipped.join(", ")}
                    </span>
                  )}
                  {served.length > 0 && (
                    <span style={{ color: "var(--color-success, #2a8a2a)" }}>
                      <strong>Served:</strong> {served.length} stop(s)
                    </span>
                  )}
                </div>
              )}

              {/* Attempts table */}
              {attempts.length > 0 && (
                <details>
                  <summary style={{ cursor: "pointer", fontWeight: 600 }}>
                    Routing attempts ({attempts.length})
                  </summary>
                  <AttemptsTable attempts={attempts} />
                </details>
              )}
            </div>
          )}
        </div>
      )}
    </section>
  );
}

import axios from "axios";

const API_BASE: string =
  (typeof import.meta.env?.VITE_API_BASE === "string" && import.meta.env.VITE_API_BASE) ||
  "http://127.0.0.1:8000/api/v1";

export type DetourV2ScoreBreakdown = {
  travel_time_s?: number | null;
  segment_penalty_s?: number | null;
  turn_penalty_s?: number | null;
  uncertainty_penalty_s?: number | null;
  service_penalty_s?: number | null;
  evidence_bonus_s?: number | null;
  sharp_turn_count?: number | null;
  skipped_stops?: number | null;
  total_score?: number | null;
  [key: string]: number | null | undefined;
};

export type DetourV2Feasibility = {
  accepted?: boolean;
  hard_reject_reasons?: string[];
  notes?: string[];
  sharp_turn_count?: number;
  confidence_score?: number | null;
  warnings?: string[];
};

export type DetourTier =
  | "AUTO_OK"
  | "REVIEW_RECOMMENDED"
  | "LOW_CONFIDENCE"
  | "EMERGENCY_FALLBACK";

export type DetourComputeStatus =
  | "auto_ok"
  | "review_recommended"
  | "low_confidence"
  | "emergency_fallback"
  | "no_safe_detour"
  | "no_impact"
  | "error";

export type DetourV2ComputeCandidate = {
  strategy?: string;
  total_score?: number | null;
  travel_time_s?: number | null;
  distance_m?: number | null;
  rejection_reasons?: string[];
  score_breakdown?: DetourV2ScoreBreakdown | null;
  feasibility?: DetourV2Feasibility | null;
  geometry_geojson?: GeoJSON.FeatureCollection | GeoJSON.Feature | null;
  summary_en?: string | null;
  tier?: DetourTier | string | null;
  confidence_score?: number | null;
  warnings?: string[];
  hard_constraints_passed?: string[];
  candidate_rank?: number | null;
  review_required?: boolean | null;
};

export type DetourV2Attempt = {
  anchor_index?: number | string | null;
  exit_stop_id?: string | null;
  rejoin_stop_id?: string | null;
  corridor?: string | null;
  candidate_count?: number | null;
  outcome?: string | null;
  reason?: string | null;
  error_type?: string | null;
  elapsed_ms?: number | null;
};

export type DetourV2Anchors = {
  exit_lon?: number;
  exit_lat?: number;
  rejoin_lon?: number;
  rejoin_lat?: number;
  exit_stop_id?: string | null;
  rejoin_stop_id?: string | null;
  exit_road_class?: string | null;
  rejoin_road_class?: string | null;
  exit_road_class_rank?: number | null;
  rejoin_road_class_rank?: number | null;
};

export type DetourV2ComputeResult = {
  status?: DetourComputeStatus | string;
  trip_id?: string;
  route_id?: string;
  error?: string | null;
  anchors?: DetourV2Anchors | null;
  corridor_stage?: string | null;
  selected?: DetourV2ComputeCandidate | null;
  candidates?: DetourV2ComputeCandidate[] | null;
  discarded?: DetourV2ComputeCandidate[] | null;
  attempts?: DetourV2Attempt[] | null;
  stitching?: {
    skipped_stop_ids?: string[];
    served_stop_ids?: string[];
    stitch_ok?: boolean;
    stitch_notes?: string[];
  } | null;
  /** Present when debug_detour=true: layers for QGIS / inspection */
  debug?: { geojson?: GeoJSON.FeatureCollection; candidate_generation?: unknown } | null;
};

export async function postDetourComputeV2(body: {
  service_date: string;
  /** Set when using explicit GTFS trip ids; omit when using route_id. */
  trip_ids?: string[];
  route_id?: string;
  direction_id?: string | null;
  blockage_geojson: GeoJSON.Geometry;
  incident_id?: number | null;
  persist?: boolean;
  debug_detour?: boolean;
  use_matched_physical?: boolean;
}): Promise<{ results: DetourV2ComputeResult[]; detour_request_ids: number[]; policy_version: string }> {
  const res = await axios.post(`${API_BASE}/detours/compute`, body);
  return res.data;
}

export async function postIncident(body: {
  polygon_geojson: GeoJSON.Geometry;
  incident_type?: string | null;
  description?: string | null;
  start_date: string;
  start_time: string;
  end_date: string;
  end_time: string;
  created_by?: string | null;
}): Promise<{
  incident_id: number;
  affected_route_count: number;
  derived_edge_ban_count: number;
  policy_version: string;
}> {
  const res = await axios.post(`${API_BASE}/incidents`, body);
  return res.data;
}

export async function getDetourPolicy(): Promise<Record<string, unknown>> {
  const res = await axios.get(`${API_BASE}/detours/policy`);
  return res.data;
}

export async function postDetourApproveV2(
  detourRequestId: number,
  body?: { approved_by?: string | null; candidate_rank?: number | null }
): Promise<{ approved_detour_id: number }> {
  const res = await axios.post(`${API_BASE}/detours/${detourRequestId}/approve`, body ?? {});
  return res.data;
}

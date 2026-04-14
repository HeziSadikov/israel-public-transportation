/**
 * GovMap raster basemap — Web Mercator (EPSG:3857) tile URLs only.
 * Obtain allowed tile templates / attribution from the GovMap operator; do not rely on undocumented endpoints.
 *
 * Modes:
 * - Direct: set VITE_GOVMAP_TILES (comma-separated XYZ templates, {z}/{x}/{y}).
 * - Same-origin proxy: set VITE_GOVMAP_USE_PROXY=1 and run the API with GOVMAP_TILE_UPSTREAM_TEMPLATE (backend).
 */

const env = import.meta.env;

function apiBase(): string {
  const v = env.VITE_API_BASE;
  if (typeof v === "string" && v.trim()) return v.trim().replace(/\/$/, "");
  return "http://127.0.0.1:8000/api/v1";
}

function useProxy(): boolean {
  const p = env.VITE_GOVMAP_USE_PROXY;
  return p === "1" || String(p).toLowerCase() === "true";
}

/** True when GovMap raster can be loaded (direct URLs or proxy mode). */
export function isGovmapBasemapConfigured(): boolean {
  if (useProxy()) return true;
  const tiles = String(env.VITE_GOVMAP_TILES ?? "").trim();
  return tiles.length > 0;
}

export function govmapRasterTiles(): string[] {
  if (useProxy()) {
    const u = `${apiBase()}/govmap-tiles/{z}/{x}/{y}`;
    return [u, u, u];
  }
  const raw = String(env.VITE_GOVMAP_TILES ?? "");
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

export function govmapAttribution(): string {
  const a = String(env.VITE_GOVMAP_ATTRIBUTION ?? "").trim();
  if (a) return a;
  return "GovMap — set VITE_GOVMAP_ATTRIBUTION to your approved attribution text";
}

export function govmapRasterMaxZoom(): number {
  const n = Number.parseInt(String(env.VITE_GOVMAP_MAXZOOM ?? "19"), 10);
  if (!Number.isFinite(n) || n < 0 || n > 24) return 19;
  return n;
}

/** GeoJSON Polygon with lon/lat positions */
export type ParsedPolygonResult =
  | { ok: true; geometry: GeoJSON.Polygon }
  | { ok: false; error: string };

/**
 * Parse textarea content into a Polygon outer ring: comma- or newline-separated segments,
 * each segment two numbers (longitude latitude), whitespace-separated.
 */
export function parseLonLatPolygonFromText(text: string): ParsedPolygonResult {
  const raw = text.trim();
  if (!raw) {
    return { ok: false, error: "Paste coordinate pairs (lon lat, lon lat, …)." };
  }

  // Normalize newlines and semicolons to commas so one pair per line works too.
  const normalized = raw.replace(/[\r\n;]+/g, ",");

  const segments = normalized
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const positions: [number, number][] = [];

  for (let i = 0; i < segments.length; i++) {
    const seg = segments[i];
    const parts = seg.split(/\s+/).filter(Boolean);
    if (parts.length === 1) {
      return {
        ok: false,
        error: `Incomplete pair before "${parts[0]}" — each vertex needs longitude and latitude.`,
      };
    }
    if (parts.length > 2) {
      return {
        ok: false,
        error: `Too many numbers in "${seg}" — use exactly two numbers per vertex (lon lat).`,
      };
    }
    const lon = Number(parts[0]);
    const lat = Number(parts[1]);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) {
      return { ok: false, error: `Invalid number in "${seg}".` };
    }
    positions.push([lon, lat]);
  }

  if (positions.length < 3) {
    return { ok: false, error: "Need at least three lon/lat pairs for a polygon." };
  }

  const ring: [number, number][] = [...positions];
  const first = ring[0];
  const last = ring[ring.length - 1];
  const closed = first[0] === last[0] && first[1] === last[1];
  if (!closed) {
    ring.push([first[0], first[1]]);
  }

  const geometry: GeoJSON.Polygon = {
    type: "Polygon",
    coordinates: [ring],
  };

  return { ok: true, geometry };
}

/** Soft bounds check for Israel-ish coordinates — UI warning only. */
export function coordinatesOutsideIsraelHint(positions: [number, number][]): string | null {
  if (!positions.length) return null;
  for (const [lon, lat] of positions) {
    if (lon < 33 || lon > 37 || lat < 28 || lat > 34) {
      return "Some vertices look outside the usual Israel bounding box; verify lon/lat order (GeoJSON: longitude first).";
    }
  }
  return null;
}

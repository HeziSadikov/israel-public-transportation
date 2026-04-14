"""
Nominatim forward geocoding (shared by /geocode and narrative detour routing).

Use NOMINATIM_URL to point at a self-hosted instance in production.
"""

from __future__ import annotations

import os
from math import asin, cos, radians, sin, sqrt
from typing import Any, Dict, List, Optional, Tuple

import httpx

# Default public endpoint; override with NOMINATIM_URL for production.
NOMINATIM_URL_DEFAULT = "https://nominatim.openstreetmap.org"

# Israel bounding box (southwest lon, southwest lat, northeast lon, northeast lat) for viewbox.
_VIEWBOX_IL = "34.05,29.35,35.95,33.45"

USER_AGENT = os.getenv("NOMINATIM_USER_AGENT", "IsraelGTFSDetourRouter/1.0")


def _haversine_m(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lon1, lat1 = radians(a[0]), radians(a[1])
    lon2, lat2 = radians(b[0]), radians(b[1])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    h = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 6371000.0 * (2 * asin(min(1.0, sqrt(h))))


def nominatim_search_raw(
    q: str,
    *,
    limit: int = 5,
    viewbox: Optional[str] = None,
    bounded: bool = False,
    countrycodes: str = "il",
    timeout_s: float = 10.0,
) -> List[Dict[str, Any]]:
    """Low-level Nominatim search; returns raw JSON list."""
    q = (q or "").strip()
    if len(q) < 2:
        return []
    base = (os.getenv("NOMINATIM_URL") or NOMINATIM_URL_DEFAULT).rstrip("/")
    params: Dict[str, Any] = {
        "q": q,
        "format": "json",
        "limit": min(max(limit, 1), 10),
        "countrycodes": countrycodes,
    }
    if viewbox:
        params["viewbox"] = viewbox
        params["bounded"] = "1" if bounded else "0"
    url = f"{base}/search"
    with httpx.Client(timeout=timeout_s, headers={"User-Agent": USER_AGENT}) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
    return data if isinstance(data, list) else []


def _viewbox_around(lon: float, lat: float, delta: float = 0.18) -> str:
    """Nominatim viewbox: southwest lon, southwest lat, northeast lon, northeast lat."""
    return f"{lon - delta},{lat - delta},{lon + delta},{lat + delta}"


def geocode_query_best_effort(
    q: str,
    *,
    prior_lonlat: Optional[Tuple[float, float]] = None,
    max_leg_m: float = 120_000.0,
    default_viewbox: str = _VIEWBOX_IL,
) -> Optional[Tuple[float, float]]:
    """
    Geocode a single query; prefer results near prior_lonlat when given.
    Returns (lon, lat) or None.
    """
    if not q or len(q.strip()) < 2:
        return None
    viewbox = default_viewbox
    bounded = True
    if prior_lonlat is not None:
        plon, plat = prior_lonlat
        viewbox = _viewbox_around(plon, plat, delta=0.22)
        bounded = True
    raw = nominatim_search_raw(q, limit=8, viewbox=viewbox, bounded=bounded)
    if not raw and prior_lonlat is not None:
        raw = nominatim_search_raw(q, limit=8, viewbox=default_viewbox, bounded=True)
    if not raw:
        return None

    candidates: List[Tuple[float, float]] = []
    for item in raw:
        lat, lon = item.get("lat"), item.get("lon")
        if lat is None or lon is None:
            continue
        try:
            candidates.append((float(lon), float(lat)))
        except (TypeError, ValueError):
            continue
    if not candidates:
        return None

    if prior_lonlat is None:
        return candidates[0]

    for c in candidates:
        if _haversine_m(prior_lonlat, c) <= max_leg_m:
            return c
    return candidates[0]


def geocode_ordered_waypoints(
    queries: List[str],
    *,
    max_leg_m: float = 120_000.0,
) -> Optional[List[Tuple[float, float]]]:
    """
    Geocode an ordered list of place queries to (lon, lat) waypoints.
    Returns None if fewer than two points could be resolved.
    """
    if len(queries) < 2:
        return None
    out: List[Tuple[float, float]] = []
    prior: Optional[Tuple[float, float]] = None
    for q in queries:
        pt = geocode_query_best_effort(q, prior_lonlat=prior, max_leg_m=max_leg_m)
        if pt is None:
            continue
        out.append(pt)
        prior = pt
    if len(out) < 2:
        return None
    return out

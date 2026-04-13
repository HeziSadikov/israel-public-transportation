"""Split operator Hebrew narrative into turn-by-turn step dicts."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


# Leading tokens removed when building Nominatim queries from free-text steps.
_TURN_PREFIX_RE = re.compile(
    r"^(?:\s*(?:שמאלה|ימינה|ישר|ישרים|פנה|המשך|עקוף|הבא|אחרי|לפני|מעל|מתחת|"
    r"צפון|דרום|מזרח|מערב|צפונה|דרומה|מזרחה|מערבה|פנו|נא)\s*)+",
    re.UNICODE,
)
_AL_EL_RE = re.compile(r"^\s*(?:אל|לכיוון|בכיוון|על|דרך|ב|מ|ל)\s+", re.UNICODE)
_SKIP_SUBSTRINGS = (
    "המסלול המקורי",
    "מסלול מקורי",
    "בחזרה אל",
    "חזרה אל",
)


def _strip_turn_boilerplate_he(fragment: str) -> str:
    s = str(fragment).strip()
    if not s:
        return ""
    for sub in _SKIP_SUBSTRINGS:
        if sub in s and len(s) < 40:
            return ""
    while True:
        t = _TURN_PREFIX_RE.sub("", s).strip()
        t = _AL_EL_RE.sub("", t).strip()
        if t == s:
            break
        s = t
    return s.strip(" ,;")


def step_dict_to_geocode_query(step: Dict[str, Any]) -> Optional[str]:
    """
    Derive a single search string for geocoding from a turn step dict.
    Prefers structured fields; otherwise strips Hebrew turn boilerplate from instruction text.
    """
    if not isinstance(step, dict):
        return None
    if step.get("street") and str(step["street"]).strip():
        return str(step["street"]).strip()
    if step.get("intersection_with") and str(step["intersection_with"]).strip():
        return str(step["intersection_with"]).strip()
    if step.get("toward_street") and str(step["toward_street"]).strip():
        return str(step["toward_street"]).strip()
    raw = step.get("instruction_he") or step.get("instruction_en") or ""
    q = _strip_turn_boilerplate_he(str(raw))
    return q if q else None


def merged_steps_to_geocode_queries(steps: List[Dict[str, Any]]) -> List[str]:
    """Ordered list of non-empty geocode queries (intersection / street names only)."""
    out: List[str] = []
    for step in steps:
        q = step_dict_to_geocode_query(step)
        if q:
            out.append(q)
    return out


def instructions_text_he_to_steps(text: str) -> List[Dict[str, Any]]:
    """
    Split free-text Hebrew (or mixed) directions into steps.
    Splits on commas, semicolons (ASCII and Arabic U+061B), and newlines.
    """
    if not text or not str(text).strip():
        return []
    parts = re.split(r"[,;\n\r\u061B]+", str(text).strip())
    out: List[Dict[str, Any]] = []
    for p in parts:
        s = p.strip()
        if s:
            out.append({"instruction_he": s})
    return out

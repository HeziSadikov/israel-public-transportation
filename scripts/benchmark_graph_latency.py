from __future__ import annotations

"""
Small latency benchmark for /graph/build cache behavior.

Examples:
  python -m scripts.benchmark_graph_latency --route-id 36594 --direction-id 1 --date 20260327
  python -m scripts.benchmark_graph_latency --route-id 36594 --direction-id 1 --date 20260327 --runs 12 --pretty-osm
"""

import argparse
import json
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

from backend.infra.logging_utils import ensure_cli_action_logging, log


def _post_json(
    url: str, payload: Dict[str, Any], timeout_s: float = 60.0
) -> Tuple[int, Dict[str, Any], float, Dict[str, str]]:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8")
            dt_ms = (time.perf_counter() - t0) * 1000.0
            data = json.loads(raw) if raw else {}
            headers = {k.lower(): v for k, v in resp.headers.items()}
            return int(resp.status), data, dt_ms, headers
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        dt_ms = (time.perf_counter() - t0) * 1000.0
        try:
            data = json.loads(raw) if raw else {"detail": raw}
        except Exception:
            data = {"detail": raw}
        return int(e.code), data, dt_ms, {}


def _get_json(url: str, timeout_s: float = 30.0) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
    req = urllib.request.Request(url=url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8")
            data = json.loads(raw) if raw else {}
            headers = {k.lower(): v for k, v in resp.headers.items()}
            return int(resp.status), data, headers
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(raw) if raw else {"detail": raw}
        except Exception:
            data = {"detail": raw}
        return int(e.code), data, {}


def _fmt(ms: float) -> str:
    return f"{ms:.1f}ms"


def main() -> None:
    ensure_cli_action_logging()
    log("benchmark_graph_latency", "phase=main start")
    ap = argparse.ArgumentParser(description="Benchmark /graph/build or /graph/preview cold-vs-warm latency.")
    ap.add_argument("--base-url", type=str, default="http://127.0.0.1:8000")
    ap.add_argument("--route-id", type=str, required=True)
    ap.add_argument("--direction-id", type=str, default=None)
    ap.add_argument("--date", type=str, required=True, help="YYYYMMDD")
    ap.add_argument("--pretty-osm", action="store_true", help="Benchmark pretty_osm=true path.")
    ap.add_argument(
        "--endpoint",
        type=str,
        default="build",
        choices=["build", "preview"],
        help="Benchmark endpoint: /graph/build (POST) or /graph/preview (GET).",
    )
    ap.add_argument("--runs", type=int, default=8, help="Total /graph/build calls (default 8).")
    ap.add_argument("--timeout-s", type=float, default=90.0)
    ap.add_argument(
        "--trigger-warmup",
        action="store_true",
        help="Call /graph/cache/warmup before benchmark.",
    )
    args = ap.parse_args()

    build_url = urllib.parse.urljoin(args.base_url.rstrip("/") + "/", "graph/build")
    preview_url = urllib.parse.urljoin(args.base_url.rstrip("/") + "/", "graph/preview")
    warmup_url = urllib.parse.urljoin(args.base_url.rstrip("/") + "/", "graph/cache/warmup")
    status_url = urllib.parse.urljoin(args.base_url.rstrip("/") + "/", "graph/cache/status")

    if args.trigger_warmup:
        log("benchmark_graph_latency", "phase=trigger_warmup start")
        st, data, dt, _ = _post_json(warmup_url, {}, timeout_s=args.timeout_s)
        print(f"warmup: status={st}, took={_fmt(dt)}")
        if st >= 400:
            print(f"warmup_error: {data}")
            log("benchmark_graph_latency", f"phase=trigger_warmup error status={st}")
        else:
            log("benchmark_graph_latency", f"phase=trigger_warmup done status={st} elapsed_ms={dt:.1f}")

    st, status_data, _ = _get_json(status_url)
    if st == 200:
        entries = status_data.get("entries")
        print(f"cache_status_before: entries={entries}")

    payload: Dict[str, Any] = {
        "route_id": args.route_id,
        "date": args.date,
        "pretty_osm": bool(args.pretty_osm),
    }
    if args.direction_id is not None and str(args.direction_id).strip() != "":
        payload["direction_id"] = str(args.direction_id)

    runs = max(2, int(args.runs))
    times: List[float] = []
    backend_times: List[float] = []
    failures = 0
    print(
        f"benchmark: endpoint={args.endpoint}, runs={runs}, route_id={args.route_id}, "
        f"direction_id={args.direction_id}, pretty_osm={args.pretty_osm}"
    )
    for i in range(runs):
        if args.endpoint == "preview":
            q = urllib.parse.urlencode(
                {
                    "route_id": payload["route_id"],
                    "direction_id": payload.get("direction_id") or "",
                    "date": payload["date"],
                    "pretty_osm": "true" if payload["pretty_osm"] else "false",
                }
            )
            st, data, dt, headers = _get_json(f"{preview_url}?{q}", timeout_s=args.timeout_s)
        else:
            st, data, dt, headers = _post_json(build_url, payload, timeout_s=args.timeout_s)
        times.append(dt)
        b_ms = float(headers.get("x-elapsed-ms", "0") or 0.0)
        backend_times.append(b_ms)
        ok = st < 400
        mark = "ok" if ok else "err"
        print(
            f"  run {i + 1:02d}: {mark} status={st} latency={_fmt(dt)} "
            f"backend={_fmt(b_ms)} cache_hit={headers.get('x-cache-hit', 'n/a')} "
            f"graph_hit={headers.get('x-graph-cache-hit', 'n/a')}"
        )
        if not ok:
            failures += 1
            detail = data.get("detail", data)
            print(f"    detail={detail}")
    log(
        "benchmark_graph_latency",
        f"phase=benchmark_loop done runs={runs} failures={failures}",
    )

    first = times[0]
    warm = times[1:]
    warm_avg = statistics.mean(warm)
    warm_p50 = statistics.median(warm)
    warm_p95 = warm_p50 if len(warm) < 2 else sorted(warm)[max(0, int(round(0.95 * (len(warm) - 1))))]
    speedup = first / warm_avg if warm_avg > 0 else 0.0
    warm_backend_avg = statistics.mean(backend_times[1:]) if len(backend_times) > 1 else backend_times[0]
    print("")
    print("summary:")
    print(f"  first_call: {_fmt(first)}")
    print(f"  warm_avg:   {_fmt(warm_avg)}")
    print(f"  warm_p50:   {_fmt(warm_p50)}")
    print(f"  warm_p95:   {_fmt(warm_p95)}")
    print(f"  backend_avg:{_fmt(warm_backend_avg)}")
    print(f"  speedup_x:  {speedup:.2f}x")
    print(f"  failures:   {failures}/{runs}")
    log("benchmark_graph_latency", "phase=main done")


if __name__ == "__main__":
    main()


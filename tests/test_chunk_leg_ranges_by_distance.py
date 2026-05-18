from backend.domain.detour_physical.pattern_trace_split import chunk_leg_ranges_by_distance


def test_distance_chunks_respect_span_cap():
    # 5 legs, 50km total -> ~10km per leg
    cum = [0.0, 10_000.0, 20_000.0, 30_000.0, 40_000.0, 50_000.0]
    ranges = chunk_leg_ranges_by_distance(
        5, cum, max_span_m=12_000.0, max_legs_per_chunk=11, overlap=1
    )
    for lo, hi in ranges:
        assert float(cum[hi + 1]) - float(cum[lo]) <= 12_000.0 + 1e-6
    assert ranges[0][0] == 0

"""
Data Siege defense. One metered call per event (budget ~1.5 credits/event avg),
then a combined-signal decision: baselines are mean +/- 3 sigma of clean data,
so we recover sigma from them and score each metric as a z-score. A single
metric past the published bound alerts; subtler faults are caught by a lower
single-metric bar plus an "several mild deviations together" rule.
"""
from api import Verdict

# z-score bars (in clean-stream sigmas)
SOLO_Z = 2.7      # one metric this far out alone -> alert
MILD_Z = 1.8      # a metric this far out counts as a "mild" deviation
MILD_COUNT = 2    # ...and this many mild deviations together -> alert

# Clean-stream means for one-sided metrics (the published bound is mean + 3 sigma,
# so sigma = (bound - mean) / 3). Measured on clean events of the practice stream.
CLEAN_MEAN = {
    "null_rate_max": 0.005, "staleness_min_max": 4.1, "freshness_delay_max_min": 5.1,
    "lineage_duration_ms_max": 4125.0, "embedding_centroid_shift_max": 0.02,
    "corpus_avg_doc_age_days_max": 26.0, "feature_mean_shift_sigma_max": 0.0,
}
# std_amount has no published baseline; clean-stream mean/sd from practice.
STD_AMOUNT = (14.8, 1.15)
# Upstream tables a healthy transform reads (seen on clean lineage runs).
HEALTHY_UPSTREAM = frozenset({"raw.orders", "raw.customers"})


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def _two_sided_z(x, lo, hi):
    mid, sigma = (lo + hi) / 2.0, (hi - lo) / 6.0
    return abs(x - mid) / sigma if sigma > 0 else 0.0


def _upper_z(x, hi, key):
    """One-sided: upward deviation only, bound = clean mean + 3 sigma."""
    mean = CLEAN_MEAN[key]
    sigma = (hi - mean) / 3.0
    return max(0.0, (x - mean) / sigma) if sigma > 0 else 0.0


def _null_z(x, hi):
    """Null rate: fault can also be a suspiciously perfect 0 (two-sided)."""
    mean = CLEAN_MEAN["null_rate_max"]
    return abs(x - mean) / ((hi - mean) / 3.0)


def _decide(zs, pillar, extra_alert=False, reason=""):
    zs = [z for z in zs if z is not None]
    top = max(zs) if zs else 0.0
    mild = sum(1 for z in zs if z >= MILD_Z)
    alert = extra_alert or top >= SOLO_Z or mild >= MILD_COUNT
    return Verdict(alert=alert, confidence=min(1.0, top / 3.0), reason=reason or f"zmax={top:.2f} mild={mild}", pillar=pillar)


def _err(r):
    return not isinstance(r, dict) or "error" in r


def check_data_batch(payload, ctx):
    b = ctx.baseline
    r = ctx.tools.batch_profile(payload["batch_id"])
    if _err(r):
        return Verdict(alert=False, pillar="checks", reason="tool error")
    nulls = r.get("null_rate", {}).get("customer_id", 0.0)
    zs = [
        _two_sided_z(r["row_count"], b["row_count_min"], b["row_count_max"]),
        _two_sided_z(r["mean_amount"], b["mean_amount_min"], b["mean_amount_max"]),
        _null_z(nulls, b["null_rate_max"]),
        _upper_z(r["staleness_min"], b["staleness_min_max"], "staleness_min_max"),
        abs(r["std_amount"] - STD_AMOUNT[0]) / STD_AMOUNT[1],
    ]
    return _decide(zs, "checks")


def check_contract_checkpoint(payload, ctx):
    b = ctx.baseline
    r = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    if _err(r):
        return Verdict(alert=False, pillar="contracts", reason="tool error")
    violations = r.get("violations") or []
    z = _upper_z(r["freshness_delay_min"], b["freshness_delay_max_min"], "freshness_delay_max_min")
    return _decide([z], "contracts", extra_alert=bool(violations), reason=f"violations={violations} fz={z:.2f}")


def check_lineage_run(payload, ctx):
    b = ctx.baseline
    r = ctx.tools.lineage_graph_slice(payload["run_id"])
    if _err(r):
        return Verdict(alert=False, pillar="lineage", reason="tool error")
    z = _upper_z(r["duration_ms"], b["lineage_duration_ms_max"], "lineage_duration_ms_max")
    # Structural checks: missing upstream edge (a strict subset of an upstream set
    # we have seen on a healthy run) or an orphaned output (nothing downstream).
    actual = frozenset(r.get("actual_upstream") or [])
    seen = ctx.state.setdefault("upstream_sets", {HEALTHY_UPSTREAM})
    structural = (
        r.get("actual_downstream_count", 1) == 0
        or not actual
        or any(actual < known for known in seen)
    )
    if not structural and z < MILD_Z:
        seen.add(actual)
    return _decide([z], "lineage", extra_alert=structural, reason=f"dz={z:.2f} structural={structural}")


def check_feature_materialization(payload, ctx):
    b = ctx.baseline
    r = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if _err(r):
        return Verdict(alert=False, pillar="ai_infra", reason="tool error")
    z = _upper_z(r["mean_shift_sigma"], b["feature_mean_shift_sigma_max"], "feature_mean_shift_sigma_max")
    return _decide([z], "ai_infra")


def check_embedding_batch(payload, ctx):
    b = ctx.baseline
    r = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if _err(r):
        return Verdict(alert=False, pillar="ai_infra", reason="tool error")
    zs = [
        _upper_z(r["centroid_shift"], b["embedding_centroid_shift_max"], "embedding_centroid_shift_max"),
        _upper_z(r["avg_doc_age_days"], b["corpus_avg_doc_age_days_max"], "corpus_avg_doc_age_days_max"),
    ]
    return _decide(zs, "ai_infra")

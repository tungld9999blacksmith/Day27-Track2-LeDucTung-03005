# Reflection

**Approach.** One metered call per event (~1.5 credits/event, well under budget). Each
metric is turned into a z-score using the published baseline (bound = clean mean + 3σ,
so σ = (bound − mean)/3). An event alerts if one metric is ≥ 2.7σ out, or two metrics are
each ≥ 1.8σ out. Contracts alert on any reported violation. Lineage alerts on a missing
upstream edge (a strict subset of a healthy upstream set), an orphaned output
(downstream count 0), or an abnormal runtime.

**Which fault types were hardest to catch, and why?**
Subtle faults. Practice was easy (obvious faults far outside the bounds), but on the
private stream recall fell to about 65% (TPR 0.648, FPR 0.014, score 32.0). The misses were
almost all subtle `corpus_staleness`, `embedding_drift`, `runtime_anomaly`,
`volume_drop/spike`, `null_spike`, `distribution_shift` and `freshness_lag`: values
inside the ±3σ envelope that a single threshold cannot separate from normal variance.
My practice calibration also under-represented how close subtle faults sit to clean data,
so my thresholds were tuned for the obvious end.

**What would I change in the cost/coverage tradeoff?**
Cost was not the binding constraint (zero overage, budget largely unused), while recall was.
I would spend the spare budget on lower-threshold detection: keep per-pillar running
statistics in `ctx.state` and use sequential tests (CUSUM/EWMA) so a persistent small
shift is caught even when no single event crosses 3σ, accepting a slightly higher FPR
(each false positive costs 0.3/n_clean, each miss 0.5/n_faulty, so a lower bar is worth
it up to roughly a 1.7× rate ratio). I would also combine more signals per event
(e.g. row_count and std_amount together, centroid shift and doc age together) rather
than treating them independently.

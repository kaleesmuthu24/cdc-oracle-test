# Generated evaluation evidence

`make evaluate` writes actual sandbox measurements here:

- `evaluation_summary.csv` — one row per experiment.
- `<experiment>.json` — detailed metrics and reconciliation evidence.
- `readiness_score.csv` — evidence-backed readiness controls.

These files are ignored by Git except for this README so that each researcher generates fresh results.

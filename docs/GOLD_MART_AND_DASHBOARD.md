# Gold/Mart Config-Driven Models and Experiment Dashboard

## Purpose

This version adds a config-driven Gold/Mart layer without dbt. The same metadata-as-control-plane principle is preserved: SQL models, targets, upstream dependencies, quality rules, partitioning and OpenMetadata lineage are declared in YAML and executed by Spark SQL or Trino.

## Files

- `metadata/gold_models.yaml`: Gold/Mart model definitions and quality rules.
- `src/jobs/gold_model_runner.py`: executes configured SQL models, writes Iceberg tables, runs model-level quality rules and emits OpenMetadata lineage.
- `metadata/dashboard_metrics.yaml`: declares dashboard sections and artifact sources.
- `src/jobs/experiment_dashboard_builder.py`: builds `results/dashboard/dashboard_summary.json` and `results/dashboard/dashboard.md`.
- `scripts/run_gold_models.sh`: runs Gold/Mart models.
- `scripts/run_build_dashboard.sh`: builds dashboard artifacts.

## Flow

```text
Silver Iceberg
   -> gold_model_runner.py
   -> Gold Iceberg / Mart Iceberg
   -> quality metrics
   -> OpenMetadata lineage
   -> dashboard builder
   -> results/dashboard/dashboard.md
```

## Commands

```bash
MODE=summary ./scripts/run_gold_models.sh
./scripts/run_gold_models.sh
MODELS=gold_daily_revenue ./scripts/run_gold_models.sh
./scripts/run_build_dashboard.sh
```

## Why this instead of dbt?

This approach avoids introducing a second transformation control plane. Spark Kappa remains the core execution engine, Airflow orchestrates, OpenMetadata catalogs, and Gold/Mart models remain driven by the same metadata configuration style used across the prototype.

"""Measure actual on-storage size of every Medallion layer and baseline bucket.

Verifies the real generated data volume per scale (thesis Table 4.4 dataset
claims) instead of relying on row-count proxies.

    spark-submit src/jobs/measure_layer_sizes.py --config metadata/pipeline_config.yaml \
        --scale 1gb --out results/layer_sizes_1gb.json
"""
from __future__ import annotations

import argparse
import json
import os

from src.common.spark_session import build_spark
from src.common.fs_utils import path_size_bytes

LAYERS = {
    "raw": "s3a://lakehouse-raw",
    "work": "s3a://lakehouse-work",
    "silver": "s3a://lakehouse-silver",
    "gold": "s3a://lakehouse-gold",
    "mart": "s3a://lakehouse-mart",
    "audit": "s3a://lakehouse-audit",
    "quarantine": "s3a://lakehouse-quarantine",
    "baseline": "s3a://lakehouse-baseline",
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--scale", default="unknown")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    spark = build_spark("measure-layer-sizes")

    sizes = {}
    for layer, root in LAYERS.items():
        b = path_size_bytes(spark, root)
        sizes[layer] = {"bytes": b, "mb": round(b / 1024 / 1024, 2), "gb": round(b / 1024**3, 3)}
    result = {"scale": args.scale, "layers": sizes,
              "total_gb": round(sum(v["bytes"] for v in sizes.values()) / 1024**3, 3)}

    out = args.out or f"results/layer_sizes_{args.scale}.json"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(result)
    spark.stop()


if __name__ == "__main__":
    main()

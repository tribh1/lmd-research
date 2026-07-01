from __future__ import annotations

import argparse
from ._utils import write_json
from . import exp1_metadata_discoverability as e1
from . import exp2_governance_coverage as e2
from . import exp3_processing_scalability as e3
from . import exp4_query_performance as e4
from . import exp5_schema_evolution as e5


def safe(label, fn, *args):
    try:
        return fn(*args)
    except Exception as ex:
        return {"experiment": label, "error": str(ex)}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--out", default="results/experiment_results.json")
    args = ap.parse_args()
    results = {
        "E1": safe("E1", e1.run, args.config),
        "E2": safe("E2", e2.run, args.config),
        "E3": safe("E3", e3.run, args.config),
        "E4": safe("E4", e4.run, args.config),
        "E5": safe("E5", e5.run, args.config),
    }
    write_json(args.out, results)
    print(results)

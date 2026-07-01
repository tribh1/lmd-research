from __future__ import annotations

import argparse
import json

from src.common.kappa_openmetadata import OpenMetadataEmitter
from src.common.kappa_registry import load_kappa_registry, kappa_registry_summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Register Kappa assets, embedded metadata contract and lineage in OpenMetadata")
    ap.add_argument("--kappa-config", default="metadata/kappa_flows.yaml")
    ap.add_argument("--openmetadata-config", default="metadata/openmetadata_config.yaml")
    ap.add_argument("--print-summary", action="store_true")
    args = ap.parse_args()

    registry = load_kappa_registry(args.kappa_config)
    emitter = OpenMetadataEmitter.from_file(args.openmetadata_config)
    emitter.register_all(registry)

    if args.print_summary:
        print(json.dumps(kappa_registry_summary(registry), indent=2, ensure_ascii=False))
    print(json.dumps({"status": "submitted", "flows": len(registry.flows), "models": len(registry.models)}, ensure_ascii=False))


if __name__ == "__main__":
    main()

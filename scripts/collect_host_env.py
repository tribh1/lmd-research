"""Record the host environment specification for thesis Table 4.3.

    python scripts/collect_host_env.py --out results/host_environment.json
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
from pathlib import Path


def _run(cmd: list[str]) -> str | None:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=15).stdout.strip() or None
    except Exception:
        return None


def _memory_gb() -> float | None:
    try:
        import psutil  # optional
        return round(psutil.virtual_memory().total / 1024**3, 1)
    except Exception:
        pass
    if platform.system() == "Linux":
        try:
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal"):
                        return round(int(line.split()[1]) / 1024**2, 1)
        except Exception:
            pass
    if platform.system() == "Windows":
        out = _run(["powershell", "-NoProfile", "-Command",
                    "(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory"])
        if out and out.isdigit():
            return round(int(out) / 1024**3, 1)
    return None


def _cpu_model() -> str | None:
    if platform.system() == "Linux":
        try:
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name"):
                        return line.split(":", 1)[1].strip()
        except Exception:
            pass
    if platform.system() == "Windows":
        return _run(["powershell", "-NoProfile", "-Command",
                     "(Get-CimInstance Win32_Processor).Name"])
    return platform.processor() or None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="results/host_environment.json")
    args = ap.parse_args()

    env = {
        "cpu_model": _cpu_model(),
        "cpu_logical_cores": os.cpu_count(),
        "memory_total_gb": _memory_gb(),
        "operating_system": f"{platform.system()} {platform.release()} ({platform.version()})",
        "python_version": platform.python_version(),
        "container_runtime": _run(["docker", "--version"]),
        "docker_compose": _run(["docker", "compose", "version"]),
        "spark_executor_config": {
            "SPARK_PACKAGES": os.getenv("SPARK_PACKAGES", "(default, see src/common/spark_session.py)"),
            "spark.sql.shuffle.partitions": "8 (set in src/common/spark_session.py)",
            "SPARK_MODE": os.getenv("SPARK_MODE"),
        },
        "note": "Storage type/capacity must be recorded manually (SSD/NVMe model).",
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(env, f, indent=2, ensure_ascii=False)
    print(json.dumps(env, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

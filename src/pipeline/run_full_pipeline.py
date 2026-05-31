#!/usr/bin/env python3
"""
WildFire Prediction — Full Pipeline (single command)
============================================================
Runs the complete pipeline: data → features → forecasting → detection → dashboard → report.

Usage:
    python -m src.pipeline.run_full_pipeline           # run all
    python -m src.pipeline.run_full_pipeline --from 4  # start from NB04
    python -m src.pipeline.run_full_pipeline --only 4  # run only NB04

This delegates to the run_pipeline.py notebook runner at project root.
"""
import subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent

def main():
    cmd = [sys.executable, str(ROOT / "run_pipeline.py")] + sys.argv[1:]
    result = subprocess.run(cmd, cwd=str(ROOT))
    sys.exit(result.returncode)

if __name__ == "__main__":
    main()

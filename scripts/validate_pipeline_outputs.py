#!/usr/bin/env python3
"""
WildFire Pipeline Output Validation Script
==========================================
Checks all required files, columns, bounds, and daily/hourly consistency.

Usage:
    python scripts/validate_pipeline_outputs.py
    python scripts/validate_pipeline_outputs.py --save-report
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure src/ is importable from project root
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.validation import run_all_checks, print_report


def main():
    parser = argparse.ArgumentParser(description="Validate WildFire pipeline outputs")
    parser.add_argument("--save-report", action="store_true",
                        help="Save consistency report to outputs/consistency_report.csv")
    args = parser.parse_args()

    checks, consistency_report = run_all_checks(ROOT)
    exit_code = print_report(checks, consistency_report)

    if args.save_report and not consistency_report.empty:
        out = ROOT / "outputs" / "consistency_report.csv"
        consistency_report.to_csv(out, index=False)
        print(f"\n  Consistency report saved: {out}")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()

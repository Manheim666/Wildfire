#!/usr/bin/env python3
"""
WildFire Prediction — Pipeline Runner
=============================================
Single entry point for all pipeline operations.

Execution modes
---------------
  --mode full      (default) Run all 6 notebooks end-to-end
  --mode train     NB1+NB2+NB3+NB4  — ingest + weather + train wildfire model
  --mode predict   NB5+NB6           — forecast + dashboard export
  --mode weather   NB1+NB3           — ingest + weather forecast only

  --script         Bypass notebooks entirely; run pure Python pipeline.
                   Combined with --mode:
                     full    → train all models + score forecast (slow, accurate)
                     train   → train all models + save champion
                     predict → load saved champion + score forecast (FAST)

Speed / tuning flags
---------------------
  --fast           Skip NB6 (climate report); cap cell timeout at 3600 s
  --retune         Force Optuna re-tuning in NB4  (sets WILDFIRE_RETUNE=1)
  --skip-shap      Skip SHAP in NB4               (sets WILDFIRE_SKIP_SHAP=1)

Other flags
-----------
  --from N         Start from notebook N (1–6)
  --only N         Run only notebook N (1–6)
  --timeout T      Per-cell timeout in seconds (0 = unlimited)
  --kernel K       Jupyter kernel name (default: python3)
  --validate       Run validate_pipeline_outputs.py after pipeline finishes

Examples
--------
  python run_pipeline.py                           # full notebook pipeline
  python run_pipeline.py --mode train              # train only (notebooks)
  python run_pipeline.py --mode predict --script   # fast pure-Python predict
  python run_pipeline.py --mode full   --script    # full pure-Python (re-trains)
  python run_pipeline.py --only 4 --retune         # re-tune wildfire model
  python run_pipeline.py --validate                # full pipeline + validate
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import traceback
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

# ── Notebook registry ─────────────────────────────────────────────────────
NOTEBOOKS = [
    "01_Data_Ingestion.ipynb",
    "02_EDA_FeatureEngineering.ipynb",
    "03_Weather_TimeSeries.ipynb",
    "04_Wildfire_Detection.ipynb",
    "05_Risk_Prediction_Dashboard.ipynb",
    "06_Climate_Report.ipynb",
]

MODE_NOTEBOOKS: dict[str, list[int]] = {
    "full":    [1, 2, 3, 4, 5, 6],
    "train":   [1, 2, 3, 4],
    "predict": [5, 6],
    "weather": [1, 3],
}

NOTEBOOK_DIR    = PROJECT_ROOT / "notebooks"
LOG_DIR         = PROJECT_ROOT / "logs"
ESTIMATED_MIN   = [15, 5, 120, 20, 5, 2]


# ═══════════════════════════════════════════════════════════════════════════
# Notebook execution path
# ═══════════════════════════════════════════════════════════════════════════

def run_notebook(nb_path: Path, timeout: int, kernel: str,
                 env_vars: dict | None = None) -> tuple[bool, float, str]:
    import nbformat
    from nbclient import NotebookClient
    from tqdm import tqdm

    nb = nbformat.read(nb_path, as_version=4)
    cell_timeout = timeout if timeout > 0 else None
    merged_env = os.environ.copy()
    if env_vars:
        merged_env.update(env_vars)

    client = NotebookClient(nb, timeout=cell_timeout, kernel_name=kernel,
                            env=merged_env)
    code_cells = [(i, c) for i, c in enumerate(nb.cells) if c.cell_type == "code"]

    t0 = time.time()
    try:
        with client.setup_kernel(cwd=str(nb_path.parent)):
            pbar = tqdm(
                code_cells, desc=f"  {nb_path.stem}", unit="cell", leave=True,
                bar_format="  {desc}: {percentage:3.0f}%|{bar:30}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
            )
            for idx, cell in pbar:
                client.execute_cell(cell, idx)
            pbar.close()
    except Exception as exc:
        tb = traceback.format_exception_only(type(exc), exc)
        return False, time.time() - t0, (tb[-1].strip() if tb else str(exc))

    nbformat.write(nb, nb_path)
    return True, time.time() - t0, ""


def run_notebooks(nb_indices: list[int], timeout: int, kernel: str,
                  env_vars: dict) -> list[tuple[str, bool, float, str]]:
    results = []
    total = len(nb_indices)
    for step, n in enumerate(nb_indices, 1):
        nb_path = NOTEBOOK_DIR / NOTEBOOKS[n - 1]
        if not nb_path.exists():
            print(f"[{step}/{total}] ✗ {NOTEBOOKS[n-1]} — FILE NOT FOUND")
            results.append((NOTEBOOKS[n - 1], False, 0.0, "File not found"))
            break
        print(f"[{step}/{total}] Running {NOTEBOOKS[n-1]}  (est. ~{ESTIMATED_MIN[n-1]} min) …")
        ok, elapsed, err = run_notebook(nb_path, timeout, kernel, env_vars)
        if ok:
            print(f"         ✓ {fmt_time(elapsed)}")
            results.append((NOTEBOOKS[n - 1], True, elapsed, ""))
        else:
            print(f"         ✗ FAILED {fmt_time(elapsed)}: {err[:200]}")
            results.append((NOTEBOOKS[n - 1], False, elapsed, err))
            break
        print()
    return results


# ═══════════════════════════════════════════════════════════════════════════
# Pure-Python execution path
# ═══════════════════════════════════════════════════════════════════════════

def run_script(mode: str, env_vars: dict) -> list[tuple[str, bool, float, str]]:
    """Call prediction_pipeline functions directly (no notebook overhead)."""
    # Propagate env vars into this process before importing
    for k, v in env_vars.items():
        os.environ[k] = v

    from src.prediction_pipeline import main as pp_main, score_only as pp_score

    results = []

    if mode in ("full", "train"):
        label = "prediction_pipeline.main() [train + score]"
        print(f"  [script] {label} …")
        t0 = time.time()
        try:
            pp_main()
            results.append((label, True, time.time() - t0, ""))
            print(f"         ✓ {fmt_time(time.time() - t0)}")
        except Exception as exc:
            err = traceback.format_exception_only(type(exc), exc)[-1].strip()
            results.append((label, False, time.time() - t0, err))
            print(f"         ✗ FAILED: {err}")

    elif mode == "predict":
        label = "prediction_pipeline.score_only() [load saved model + score]"
        print(f"  [script] {label} …")
        t0 = time.time()
        try:
            pp_score()
            results.append((label, True, time.time() - t0, ""))
            print(f"         ✓ {fmt_time(time.time() - t0)}")
        except Exception as exc:
            err = traceback.format_exception_only(type(exc), exc)[-1].strip()
            results.append((label, False, time.time() - t0, err))
            print(f"         ✗ FAILED: {err}")

    else:
        results.append((f"script mode={mode}", False, 0.0,
                        f"--script supports modes: full, train, predict (not '{mode}')"))

    return results


# ═══════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════

def run_validation() -> bool:
    val = PROJECT_ROOT / "scripts" / "validate_pipeline_outputs.py"
    if not val.exists():
        print("  ⚠ Validation script not found.")
        return True
    print("\n  Running output validation …")
    return subprocess.run([sys.executable, str(val), "--save-report"],
                         cwd=str(PROJECT_ROOT)).returncode == 0


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def fmt_time(s: float) -> str:
    m, sec = divmod(int(s), 60)
    return f"{m}m {sec}s" if m else f"{sec}s"


def print_summary(results: list[tuple[str, bool, float, str]],
                  pipeline_elapsed: float) -> None:
    n_ok   = sum(1 for _, ok, _, _ in results if ok)
    n_fail = sum(1 for _, ok, _, _ in results if not ok)
    print()
    print("=" * 64)
    print("  PIPELINE SUMMARY")
    print("=" * 64)
    for name, ok, elapsed, err in results:
        print(f"  {'✓' if ok else '✗'} {name:45s} {fmt_time(elapsed):>8s}")
        if not ok and err:
            print(f"    Error: {err[:120]}")
    print("-" * 64)
    print(f"  Total: {n_ok} passed, {n_fail} failed — {fmt_time(pipeline_elapsed)}")
    print("=" * 64)


def write_log(results: list, pipeline_elapsed: float, args: argparse.Namespace) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = LOG_DIR / f"pipeline_{time.strftime('%Y%m%d_%H%M%S')}.log"
    with open(log, "w") as f:
        f.write(f"WildFire Pipeline — {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"mode={args.mode}  script={args.script}  fast={args.fast}\n")
        f.write("=" * 64 + "\n")
        for name, ok, elapsed, err in results:
            f.write(f"{'OK' if ok else 'FAIL':4s}  {name:45s}  {fmt_time(elapsed):>8s}\n")
            if not ok and err:
                f.write(f"      {err}\n")
        f.write("=" * 64 + "\n")
        f.write(f"Total time: {fmt_time(pipeline_elapsed)}\n")
    print(f"\n  Log: {log}")


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="WildFire — single pipeline entry point",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--mode", choices=["full", "train", "predict", "weather"],
                        default="full",
                        help="Pipeline mode (default: full)")
    parser.add_argument("--script", action="store_true",
                        help="Pure Python execution — bypass notebooks entirely")
    parser.add_argument("--from", dest="start_from", type=int, default=0,
                        help="Notebook mode: start from notebook N (1–6)")
    parser.add_argument("--only", type=int, default=0,
                        help="Notebook mode: run only notebook N (1–6)")
    parser.add_argument("--timeout", type=int, default=0,
                        help="Notebook mode: per-cell timeout seconds (0=unlimited)")
    parser.add_argument("--kernel", default="python3",
                        help="Notebook mode: Jupyter kernel (default: python3)")
    parser.add_argument("--fast", action="store_true",
                        help="Skip NB6 (climate report); cap timeout at 3600 s")
    parser.add_argument("--retune", action="store_true",
                        help="Force Optuna re-tuning (sets WILDFIRE_RETUNE=1)")
    parser.add_argument("--skip-shap", dest="skip_shap", action="store_true",
                        help="Skip SHAP (sets WILDFIRE_SKIP_SHAP=1)")
    parser.add_argument("--validate", action="store_true",
                        help="Run validate_pipeline_outputs.py after pipeline")
    args = parser.parse_args()

    # Build env vars
    env_vars: dict[str, str] = {}
    if args.retune:
        env_vars["WILDFIRE_RETUNE"] = "1"
    if args.skip_shap:
        env_vars["WILDFIRE_SKIP_SHAP"] = "1"

    timeout = args.timeout
    if args.fast and timeout == 0:
        timeout = 3600

    # Header
    exec_mode = "script (pure Python)" if args.script else "notebooks"
    print("=" * 64)
    print("  WildFire Prediction — Pipeline Runner")
    print("=" * 64)
    print(f"  Mode      : {args.mode}" + (" [fast]" if args.fast else ""))
    print(f"  Execution : {exec_mode}")
    if env_vars:
        print(f"  Env flags : {env_vars}")
    print("=" * 64)
    print()

    t0 = time.time()

    if args.script:
        # ── Pure-Python path ──────────────────────────────────────────────
        results = run_script(args.mode, env_vars)

    else:
        # ── Notebook path ─────────────────────────────────────────────────
        if args.only > 0:
            nb_indices = [args.only]
        elif args.start_from > 0:
            nb_indices = [n for n in MODE_NOTEBOOKS[args.mode] if n >= args.start_from]
        else:
            nb_indices = list(MODE_NOTEBOOKS[args.mode])

        if args.fast and 6 in nb_indices:
            nb_indices.remove(6)

        results = run_notebooks(nb_indices, timeout, args.kernel, env_vars)

    pipeline_elapsed = time.time() - t0
    print_summary(results, pipeline_elapsed)
    write_log(results, pipeline_elapsed, args)

    validation_ok = True
    if args.validate and all(ok for _, ok, _, _ in results):
        validation_ok = run_validation()

    if any(not ok for _, ok, _, _ in results) or not validation_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()

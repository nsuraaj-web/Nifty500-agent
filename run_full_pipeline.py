import subprocess
import sys
from pathlib import Path
from datetime import datetime

# Root directory of the project (folder where this file lives)
ROOT = Path(__file__).resolve().parent

# Ordered list of pipeline steps
PIPELINE_STEPS = [
    ("Yahoo Finance staging", "yfin_staging.py"),
    ("Screener staging", "screenr_stage.py"),
    ("Yahoo Finance stage → master", "yfin_stage_to_mstr.py"),
    ("Screener stage → master", "screenr_stage_to_mstr.py"),
    ("Derived values calculation", "calc_derived_value.py"),
    ("Ratings calculation", "calc_ratings.py"),
]


def run_step(name: str, script_name: str) -> None:
    """Run a single Python script as a subprocess, fail fast on error."""
    script_path = ROOT / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    print()
    print("=" * 80)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting step: {name}")
    print(f"Running: {sys.executable} {script_path}")
    print("=" * 80)

    # Run the script with the same Python interpreter (venv aware)
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(ROOT),
        check=False,  # We'll handle errors ourselves
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Step '{name}' failed with exit code {result.returncode}. "
            f"Script: {script_name}"
        )

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Completed: {name}")


def main():
    print("=" * 80)
    print("STOCK INTELLIGENCE PIPELINE")
    print("Steps:")
    for idx, (name, script) in enumerate(PIPELINE_STEPS, start=1):
        print(f"  {idx}. {name}  ({script})")
    print("=" * 80)

    for name, script in PIPELINE_STEPS:
        run_step(name, script)

    print()
    print("=" * 80)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🎉 All pipeline steps completed successfully.")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print()
        print("❌ PIPELINE FAILED")
        print("-" * 80)
        print(str(e))
        print("-" * 80)
        sys.exit(1)

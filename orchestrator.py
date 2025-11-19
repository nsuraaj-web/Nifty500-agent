import subprocess
import sys
from pathlib import Path
from datetime import datetime
import time
import requests
import os

# -----------------------------------------------------
# CONFIG
# -----------------------------------------------------

ROOT = Path(__file__).resolve().parent

# Ordered ETL + RAG pipeline
PIPELINE_STEPS = [
    ("Yahoo Finance staging", "yfin_staging.py"),
    ("Screener staging", "screenr_stage.py"),
    ("Yahoo Finance stage → master", "yfin_stage_to_mstr.py"),
    ("Screener stage → master", "screenr_stage_to_mstr.py"),
    ("Derived values calculation", "calc_derived_value.py"),
    ("Ratings calculation", "calc_ratings.py"),
    ("RAG Vectorstore Update", "ingest_supabase.py"),   # IMPORTANT
]

# Telegram notification settings (injected via env vars on GitHub Actions)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


# -----------------------------------------------------
# TELEGRAM NOTIFICATION
# -----------------------------------------------------
def notify(msg: str):
    """Send Telegram message if credentials exist. Safe to fail silently."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️  Telegram credentials not set. Skipping notifications.")
        return

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"⚠️ Telegram API error: {resp.status_code} {resp.text}")
        else:
            print("📨 Telegram notification sent")
    except Exception as e:
        print(f"⚠️ Failed to send Telegram message: {e}")


# -----------------------------------------------------
# EXECUTION OF EACH STEP
# -----------------------------------------------------
def run_step(name: str, script_name: str) -> float:
    """Run a single Python script as a subprocess, return duration in seconds."""
    script_path = ROOT / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    print("\n" + "=" * 80)
    print(f"🚀 STARTING: {name}")
    print(f"➡️  Running: {sys.executable} {script_path}")
    print("=" * 80)

    start = time.perf_counter()

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(ROOT),
        check=False,
    )

    duration = time.perf_counter() - start

    if result.returncode != 0:
        raise RuntimeError(
            f"❌ Step failed: {name} "
            f"(exit code {result.returncode}, duration {duration:.1f}s)"
        )

    print(f"✅ COMPLETED: {name} in {duration:.1f} seconds")
    return duration


# -----------------------------------------------------
# MAIN PIPELINE
# -----------------------------------------------------
def main():
    print("=" * 80)
    print("📊 STOCK INTEL PIPELINE — FULL REFRESH")
    print("=" * 80)

    for idx, (name, script) in enumerate(PIPELINE_STEPS, start=1):
        print(f"{idx}. {name} ({script})")

    print("=" * 80)

    step_timings = []
    pipeline_start = time.perf_counter()

    for name, script in PIPELINE_STEPS:
        duration = run_step(name, script)
        step_timings.append((name, duration))

    total_duration = time.perf_counter() - pipeline_start

    # Build a nice summary text
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
    lines = [
        "🎉 STOCK PIPELINE COMPLETED SUCCESSFULLY",
        f"Time: {now}",
        f"Total duration: {total_duration:.1f} seconds",
        "",
        "Step timings:",
    ]
    for name, dur in step_timings:
        lines.append(f" - {name}: {dur:.1f}s")

    summary = "\n".join(lines)

    print("\n" + "=" * 80)
    print(summary)
    print("=" * 80)

    notify(summary)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        msg = f"❌ PIPELINE FAILED\n{str(e)}"
        print("\n" + "=" * 80)
        print(msg)
        print("=" * 80)
        notify(msg)
        sys.exit(1)

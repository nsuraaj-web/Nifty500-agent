import subprocess
import sys
from pathlib import Path
from datetime import datetime
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
    ("RAG Vectorstore Update", "ingest_supabase.py"),   # NEW & IMPORTANT
]

# Telegram notification settings
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


# -----------------------------------------------------
# TELEGRAM NOTIFICATION
# -----------------------------------------------------
def notify(msg: str):
    """Send Telegram message if credentials exist."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️  Telegram credentials not set. Skipping notifications.")
        return

    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=10,
        )
        print("📨 Telegram notification sent")
    except Exception as e:
        print(f"⚠️ Failed to send Telegram message: {e}")


# -----------------------------------------------------
# EXECUTION OF EACH STEP
# -----------------------------------------------------
def run_step(name: str, script_name: str) -> None:
    script_path = ROOT / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    print("\n" + "=" * 80)
    print(f"🚀 STARTING: {name}")
    print(f"➡️  Running: {sys.executable} {script_path}")
    print("=" * 80)

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(ROOT),
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(f"❌ Step failed: {name} (exit code {result.returncode})")

    print(f"✅ COMPLETED: {name}")


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

    for name, script in PIPELINE_STEPS:
        run_step(name, script)

    # SUCCESS
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    success_msg = f"🎉 STOCK PIPELINE COMPLETED SUCCESSFULLY\nTime: {now}"
    print(success_msg)
    notify(success_msg)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        error_msg = f"❌ PIPELINE FAILED\n{str(e)}"
        print(error_msg)
        notify(error_msg)
        sys.exit(1)

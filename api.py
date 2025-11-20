# api.py
import sys
import subprocess
from pathlib import Path

from fastapi import FastAPI, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from agent import run_agent, extract_final_text  # <- same as before

# -------------------------------------------------------------------
# App setup
# -------------------------------------------------------------------
app = FastAPI(title="Stock Intelligence Agent API")

# Folder where this file lives (Render's /opt/render/project/src)
BASE_DIR = Path(__file__).resolve().parent

# Serve static documentation from /docs folder
app.mount(
    "/docs-static",
    StaticFiles(directory=str(BASE_DIR / "docs"), html=True),
    name="docs-static",
)

# -------------------------------------------------------------------
# Models
# -------------------------------------------------------------------
class AgentRequest(BaseModel):
    query: str


class AgentResponse(BaseModel):
    answer: str


# -------------------------------------------------------------------
# Health endpoint
# -------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


# -------------------------------------------------------------------
# Agent endpoint
# -------------------------------------------------------------------
@app.post("/agent", response_model=AgentResponse)
def agent_endpoint(req: AgentRequest):
    """
    Call the stock intelligence agent.

    Example:
    {
      "query": "Give me a full analysis of TCS including fundamentals, news and external commentary."
    }
    """
    result = run_agent(req.query)
    answer = extract_final_text(result)
    return AgentResponse(answer=answer)


# -------------------------------------------------------------------
# NEW: Run pipeline (orchestrator) endpoint
# -------------------------------------------------------------------
@app.post("/run-pipeline")
async def run_pipeline(background_tasks: BackgroundTasks):
    """
    Kick off the stock pipeline (orchestrator.py) in the background.

    Assumptions:
    - orchestrator.py is in the same folder as api.py (BASE_DIR)
    - orchestrator.py runs all steps:
        yfin_staging.py
        screenr_stage.py
        yfin_stage_to_mstr.py
        screenr_stage_to_mstr.py
        calc_derived_value.py
        calc_ratings.py
        ingest_supabase.py
    """

    def _run():
        try:
            cmd = [sys.executable, str(BASE_DIR / "orchestrator.py")]
            print(f"[PIPELINE] Running: {' '.join(cmd)}", flush=True)
            subprocess.run(
                cmd,
                cwd=str(BASE_DIR),
                check=True,
            )
            print("[PIPELINE] Completed successfully", flush=True)
        except Exception as e:
            print(f"[PIPELINE ERROR] {e}", flush=True)

    # Run in background so API returns immediately
    background_tasks.add_task(_run)

    return {
        "status": "started",
        "message": "Pipeline started in background on backend.",
    }

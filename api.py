# api.py
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles


from agent import run_agent, extract_final_text  # <- NOTE: no dot here

app = FastAPI(title="Stock Intelligence Agent API")

# Serve static documentation from /docs folder
app.mount(
    "/docs-static",
    StaticFiles(directory="docs", html=True),
    name="docs-static",
)


class AgentRequest(BaseModel):
    query: str


class AgentResponse(BaseModel):
    answer: str


@app.get("/health")
def health():
    return {"status": "ok"}


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

# orchestrator_api.py
from fastapi import FastAPI
from pydantic import BaseModel
from orchestrate import process_customer  # reuse your existing function

app = FastAPI(title="Orchestrator API")

class ProcessRequest(BaseModel):
    sk_id: int

class ProcessResponse(BaseModel):
    status: str
    sk_id: int

@app.get("/healthz")
def health():
    return {"status": "ok"}

@app.post("/process_customer", response_model=ProcessResponse)
def process(request: ProcessRequest):
    process_customer(request.sk_id)
    return {"status": "processed", "sk_id": request.sk_id}

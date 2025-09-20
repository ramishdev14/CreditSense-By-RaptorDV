# orchestrator_api.py
from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import Optional
from orchestrate import process_customer  # uses your existing function

app = FastAPI(title="Orchestrator API")

class ProcessResponse(BaseModel):
    status: str
    sk_id: int

@app.get("/healthz")
def health():
    return {"status": "ok"}

@app.post("/process_customer", response_model=ProcessResponse)
def process(sk_id: int = Query(..., description="SK_ID_CURR to process")):
    process_customer(sk_id)
    return {"status": "processed", "sk_id": sk_id}

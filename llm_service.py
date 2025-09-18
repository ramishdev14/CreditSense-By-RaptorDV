from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import random

app = FastAPI()

class DQRequest(BaseModel):
    table_name: str
    column_name: str
    check_type: str
    check_details: str

@app.post("/analyze")
def analyze_check(request: DQRequest):
    """Dummy LLM response for testing the full pipeline."""
    suggestion = f"For {request.column_name}, validate {request.check_type.lower()} and enforce stricter quality rules."
    return {
        "issue_description": request.check_details,
        "ai_suggestion": suggestion,
        "confidence_score": round(random.uniform(0.7, 0.95), 2)
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

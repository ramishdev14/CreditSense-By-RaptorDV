from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from dotenv import load_dotenv
import os
import re
import json

# Load env vars
load_dotenv("variables.env")

HF_TOKEN = os.getenv("HUGGING_FACE")
MODEL_NAME = "mistralai/Mistral-7B-Instruct-v0.2"

app = FastAPI()

# -------------------------------
# Global model/pipeline cache
# -------------------------------
generator = None

def get_generator():
    global generator
    if generator is None:
        print("⏳ Loading Mistral 7B Instruct...")
        tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, token=HF_TOKEN)
        model = AutoModelForCausalLM.from_pretrained(
            MODEL_NAME,
            torch_dtype=torch.float16,
            device_map="auto",
            token=HF_TOKEN
        )
        generator = pipeline(
            "text-generation",
            model=model,
            tokenizer=tokenizer,
            torch_dtype=torch.float16
        )
    return generator

# -------------------------------
# Request schema
# -------------------------------
class DQRequest(BaseModel):
    table_name: str
    column_name: str
    column_desc: str
    check_summary: str  # profiling summary (already condensed in orchestrator)

# -------------------------------
# Canonical Prompt Template
# -------------------------------
JSON_SCHEMA = """
{
  "dq_dimension": "Completeness|Validity|Uniqueness|Consistency|Timeliness",
  "suggestion": "Short, actionable fix",
  "rule_template_sql": "Optional SQL/pseudocode",
  "severity": "low|medium|high",
  "confidence": 0.0,
  "rationale": "1-2 sentence reasoning",
  "anomaly_signature": "Condition that identifies anomaly",
  "root_cause_hypothesis": "Likely source and reason",
  "lineage_hypothesis": [
    {
      "from_table": "string",
      "to_table": "string",
      "key": "string",
      "reason": "string"
    }
  ],
  "follow_up_checks": ["list of recommended checks"]
}
"""

PROMPT_TEMPLATE = """
You are an expert data quality analyst for a financial services company. 
Your task is to analyze data profiling summaries and provide structured, actionable data quality suggestions. 
DO NOT use raw PII or sensitive data. Only use the provided aggregate statistics and metadata.

### DATA CONTEXT
- Platform: Snowflake (but response must be platform-agnostic)
- Dataset Focus: Home Credit (credit risk)
- Core Tables & Relationships:
  - SAMPLE_APPLICATIONS links to SAMPLE_BUREAU on SK_ID_CURR
  - SAMPLE_APPLICATIONS links to SAMPLE_PREVIOUS_APP on SK_ID_CURR
  - SAMPLE_PREVIOUS_APP links to SAMPLE_INSTALLMENTS on SK_ID_PREV

### TASK
Based on the profiling metrics, identify anomalies, suggest fixes, propose root-cause hypothesis, and trace potential lineage. 
Output MUST be a single valid JSON object that strictly follows this schema:
{schema}

### COLUMN CONTEXT
- Table: {table_name}
- Column: {column_name} ({column_desc})
- Profiling Summary: {check_summary}

### RESPONSE
Return only JSON, no extra text.
"""

# -------------------------------
# Endpoint
# -------------------------------
@app.post("/analyze")
def analyze_check(request: DQRequest):
    generator = get_generator()

    # Fill prompt template
    prompt = PROMPT_TEMPLATE.format(
        schema=JSON_SCHEMA,
        table_name=request.table_name,
        column_name=request.column_name,
        column_desc=request.column_desc,
        check_summary=request.check_summary
    )

    print("\n================= PROMPT SENT TO MODEL =================")
    print(prompt)
    print("========================================================\n")

    result = generator(
        prompt,
        max_new_tokens=300,
        temperature=0.3,
        do_sample=False
    )

    raw_output = result[0]["generated_text"]
    print("\n================= RAW MODEL OUTPUT =================")
    print(raw_output)
    print("====================================================\n")

    # Extract JSON safely
    try:
        json_start = raw_output.find("{")
        json_end = raw_output.rfind("}") + 1
        suggestion_json = json.loads(raw_output[json_start:json_end])
    except Exception as e:
        print(f"⚠️ JSON parse failed: {e}")
        suggestion_json = {
            "dq_dimension": "Unknown",
            "suggestion": "Abstain",
            "rule_template_sql": None,
            "severity": "low",
            "confidence": 0.0,
            "rationale": "Failed to parse model output",
            "anomaly_signature": None,
            "root_cause_hypothesis": None,
            "lineage_hypothesis": [],
            "follow_up_checks": []
        }

    return suggestion_json

# -------------------------------
# Entry point
# -------------------------------
if __name__ == "__main__":
    uvicorn.run(
        "llm_service:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )

# llm_service_gemini.py
from fastapi import FastAPI
from pydantic import BaseModel
import google.generativeai as genai
import os
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv("variables.env")
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

# Use Gemini model (choose pro or flash depending on speed/quality tradeoff)
MODEL_NAME = "gemini-1.5-flash"
model = genai.GenerativeModel(MODEL_NAME)

app = FastAPI(title="Gemini LLM Service")

# -------------------------------
# Request schema
# -------------------------------
class CombinedCheck(BaseModel):
    table_name: str
    column_name: str
    column_desc: str
    check_summary: str

class CombinedRequest(BaseModel):
    customer_id: str
    all_checks: list[CombinedCheck]

# -------------------------------
# Prompt template
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
Your task is to analyze profiling summaries for a single customer and provide structured, actionable suggestions.
Focus on root cause, anomaly signature, and lineage.

### DATA CONTEXT
- Platform: Snowflake (platform-agnostic response required)
- Dataset Focus: Home Credit (credit risk)
- Core Tables & Relationships:
  - SAMPLE_APPLICATION links to SAMPLE_BUREAU on SK_ID_CURR
  - SAMPLE_APPLICATION links to SAMPLE_PREVIOUS_APP on SK_ID_CURR
  - SAMPLE_PREVIOUS_APP links to SAMPLE_INSTALLMENTS on SK_ID_PREV

### TASK
Based on the profiling summaries below, identify anomalies, propose fixes, hypothesize a single root cause, and trace lineage if possible.
Output MUST be valid JSON following this schema:
{schema}

### CUSTOMER CONTEXT
- Customer ID: {customer_id}
- Combined Summaries:
{combined_summary}

### RESPONSE
Return only JSON, no extra text.
"""

# -------------------------------
# Endpoint
# -------------------------------
@app.post("/analyze_combined")
def analyze_combined(request: CombinedRequest):
    combined_summary = ""
    for check in request.all_checks:
        combined_summary += f"- Table: {check.table_name}, Column: {check.column_name} ({check.column_desc})\n"
        combined_summary += f"  Check Summary: {check.check_summary}\n\n"

    prompt = PROMPT_TEMPLATE.format(
        schema=JSON_SCHEMA,
        customer_id=request.customer_id,
        combined_summary=combined_summary
    )

    print("\n================= PROMPT SENT TO GEMINI =================")
    print(prompt)
    print("=========================================================\n")

    try:
        response = model.generate_content(prompt)
        raw_output = response.text
        print("\n================= RAW MODEL OUTPUT =================")
        print(raw_output)
        print("====================================================\n")

        # Extract JSON
        json_start = raw_output.find("{")
        json_end = raw_output.rfind("}") + 1
        suggestion_json = json.loads(raw_output[json_start:json_end])
    except Exception as e:
        print(f"⚠️ Gemini parse failed: {e}")
        suggestion_json = {
            "dq_dimension": "Unknown",
            "suggestion": "Abstain",
            "rule_template_sql": None,
            "severity": "low",
            "confidence": 0.0,
            "rationale": "Failed to parse Gemini output",
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
    import uvicorn
    uvicorn.run("llm_service_gemini:app", host="0.0.0.0", port=8001, reload=True)

from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from dotenv import load_dotenv
import os
import re

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
        print("⏳ Loading Mistral 7B Instruct... (first run will download ~13GB)")
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
    summary: str

# -------------------------------
# Endpoint
# -------------------------------
@app.post("/analyze")
def analyze_check(request: DQRequest):
    generator = get_generator()

    prompt = f"""
You are a data quality assistant for a financial institution.

Issue Summary:
{request.summary}

Task:
Provide ONE short, clear suggestion in plain English to fix or validate this issue.
Be concrete (e.g., "Replace invalid values with NULL", "Cap outliers at threshold").
Do not use filler, underscores, or non-English characters.

Suggestion:
"""

    print("\n================= PROMPT SENT TO MODEL =================")
    print(prompt)
    print("========================================================\n")

    result = generator(
        prompt,
        max_new_tokens=80,
        temperature=0.5,
        do_sample=True,
        top_p=0.9,
        repetition_penalty=1.2
    )

    raw_output = result[0]["generated_text"]
    print("\n================= RAW MODEL OUTPUT =================")
    print(raw_output)
    print("====================================================\n")

    suggestion = raw_output.replace(prompt, "").strip()
    suggestion = re.sub(r"[^a-zA-Z0-9.,:;!?()/%$€\-\s]", "", suggestion)

    if len(suggestion) < 5:
        suggestion = "Apply a standard data quality fix (e.g., remove, impute, or flag anomalous values)."

    return {
        "issue_summary": request.summary,
        "ai_suggestion": suggestion,
        "confidence_score": 0.85
    }

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

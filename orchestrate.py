import os
import json
import requests
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

# -------------------------------
# Load env vars
# -------------------------------
load_dotenv("variables.env")
LLM_API_URL = os.getenv(
    "LLM_API_URL",
    "http://llm-gemini:8001/analyze_combined"  # ✅ safe default
)

# -------------------------------
# Connect to Snowflake
# -------------------------------
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)
cur = conn.cursor()

# -------------------------------
# Simple severity classifier
# -------------------------------
def classify_issue(issue_type, value=None, pct=None):
    if issue_type == "MISSING" and pct is not None:
        if pct > 0.3: return "High"
        elif pct > 0.1: return "Medium"
        else: return "Low"
    if issue_type == "NEGATIVE" and value is not None: return "High"
    if issue_type == "DUPLICATE": return "High"
    if issue_type == "OUTLIER": return "Medium"
    return "Low"

# -------------------------------
# Fetch customer data
# -------------------------------
def fetch_customer_data(sk_id):
    app_df = pd.read_sql(f"SELECT * FROM SAMPLE_APPLICATION WHERE SK_ID_CURR = {sk_id}", conn)
    bureau_df = pd.read_sql(f"SELECT * FROM SAMPLE_BUREAU WHERE SK_ID_CURR = {sk_id}", conn)
    return app_df, bureau_df

# -------------------------------
# Process one customer
# -------------------------------
def process_customer(sk_id: int):
    # 🧹 Delete old anomalies & suggestions
    cur.execute("DELETE FROM DQ_ANOMALIES WHERE SK_ID_CURR = %s", (sk_id,))
    cur.execute("DELETE FROM DQ_AI_SUGGESTIONS WHERE SK_ID_CURR = %s", (sk_id,))
    conn.commit()

    app_df, bureau_df = fetch_customer_data(sk_id)
    all_checks = []

    # --- Application table checks ---
    for col in ["AMT_ANNUITY", "AMT_CREDIT", "AMT_INCOME_TOTAL", "DAYS_EMPLOYED"]:
        vals = app_df[col].dropna()
        missing_pct = 1 - len(vals) / len(app_df) if len(app_df) else 0
        if missing_pct > 0:
            sev = classify_issue("MISSING", pct=missing_pct)
            all_checks.append(("SAMPLE_APPLICATION", col, "MISSING", sev, round(missing_pct*100, 1)))
        for v in vals:
            if v < 0:
                sev = classify_issue("NEGATIVE", value=v)
                all_checks.append(("SAMPLE_APPLICATION", col, "NEGATIVE", sev, v))

    # --- Bureau table checks ---
    for col in ["AMT_CREDIT_SUM", "AMT_ANNUITY"]:
        vals = bureau_df[col].dropna()
        missing_pct = 1 - len(vals) / len(bureau_df) if len(bureau_df) else 0
        if missing_pct > 0:
            sev = classify_issue("MISSING", pct=missing_pct)
            all_checks.append(("SAMPLE_BUREAU", col, "MISSING", sev, round(missing_pct*100, 1)))
        for v in vals:
            if v < 0:
                sev = classify_issue("NEGATIVE", value=v)
                all_checks.append(("SAMPLE_BUREAU", col, "NEGATIVE", sev, v))

    # -------------------------------
    # Insert anomalies & build payload
    # -------------------------------
    payload_checks = []
    for table_name, col, issue_type, severity, detail in all_checks:
        cur.execute("""
            SELECT DESCRIPTION 
            FROM COLUMN_DICTIONARY 
            WHERE TABLE_NAME=%s AND ROW_NAME=%s
        """, (table_name, col))
        desc_row = cur.fetchone()
        column_desc = desc_row[0] if desc_row else "No description available"

        anomaly_details = {"issue_type": issue_type, "detail": detail}
        cur.execute("""
            INSERT INTO DQ_ANOMALIES
            (TABLE_NAME, COLUMN_NAME, SK_ID_CURR, SK_ID_PREV, ANOMALY_TYPE, ANOMALY_DETAILS, TIMESTAMP)
            SELECT %s, %s, %s, %s, %s, PARSE_JSON(%s), CURRENT_TIMESTAMP
        """, (
            table_name, col, int(sk_id), None,
            issue_type, json.dumps(anomaly_details)
        ))
        conn.commit()

        payload_checks.append({
            "table": table_name,
            "column": col,
            "desc": column_desc,
            "summary": f"{severity} severity {issue_type} issue: {detail}"
        })

    # -------------------------------
    # Send combined payload to LLM
    # -------------------------------
    payload = {"sk_id": int(sk_id), "issues": payload_checks}
    print(f"\n📤 Sending to LLM API: {LLM_API_URL}")
    print(f"Payload:\n{json.dumps(payload, indent=2)}\n")

    try:
        resp = requests.post(LLM_API_URL, json=payload)
        print(f"📥 Response status: {resp.status_code}")
        resp.raise_for_status()
        llm = resp.json()
        print(f"📥 Raw LLM response: {json.dumps(llm, indent=2)}")

        raw_output = llm.get("raw_output", "")
        suggestions = llm.get("parsed_json", [])
        if isinstance(suggestions, dict):
            suggestions = [suggestions]

        for suggestion in suggestions:
            cur.execute("""
                INSERT INTO DQ_AI_SUGGESTIONS
                (TABLE_NAME, COLUMN_NAME, SK_ID_CURR, ISSUE_DESCRIPTION, RAW_LLM_OUTPUT, AI_SUGGESTION, 
                 CONFIDENCE_SCORE, ROOT_CAUSE_HYPOTHESIS, LINEAGE_HYPOTHESIS, TIMESTAMP)
                SELECT %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s, PARSE_JSON(%s), CURRENT_TIMESTAMP
            """, (
                payload_checks[0]["table"],
                payload_checks[0]["column"],
                int(sk_id),
                json.dumps(payload_checks),
                raw_output,
                json.dumps(suggestion),
                suggestion.get("confidence", 0.0),
                suggestion.get("root_cause_hypothesis"),
                json.dumps(suggestion.get("lineage_hypothesis", []))
            ))
            conn.commit()

        print(f"✅ Stored {len(suggestions)} AI suggestion(s) for customer {sk_id}")

    except Exception as e:
        print(f"❌ Error sending to LLM or inserting suggestions: {e}")

# -------------------------------
# MAIN (for standalone runs)
# -------------------------------
if __name__ == "__main__":
    test_sk_id = 171559
    process_customer(test_sk_id)
    cur.close()
    conn.close()

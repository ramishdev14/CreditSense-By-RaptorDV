# orchestrator_refactored.py
import os
import json
import requests
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

# -------------------------------
# Load env vars & connect to Snowflake
# -------------------------------
load_dotenv("variables.env")

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
# Simple severity classifier (rule-based for now)
# -------------------------------
def classify_issue(issue_type, value=None, pct=None):
    if issue_type == "MISSING" and pct is not None:
        if pct > 0.3:
            return "High"
        elif pct > 0.1:
            return "Medium"
        else:
            return "Low"
    if issue_type == "NEGATIVE" and value is not None:
        return "High"
    if issue_type == "DUPLICATE":
        return "High"
    if issue_type == "OUTLIER":
        return "Medium"
    return "Low"

# -------------------------------
# Anomaly summarizer for LLM
# -------------------------------
def make_summary(table_name, column_name, column_desc, issue_type, severity, detail):
    if issue_type == "MISSING":
        return f"{severity} severity: {detail}% of values in {column_name} are missing in {table_name}."
    if issue_type == "NEGATIVE":
        return f"{severity} severity: Negative value {detail} found in {column_name} in {table_name}."
    if issue_type == "DUPLICATE":
        return f"{severity} severity: Duplicate SK_ID found in {table_name} ({detail})."
    if issue_type == "OUTLIER":
        return f"{severity} severity: Outlier value {detail} detected in {column_name} in {table_name}."
    return f"{severity} severity issue in {column_name} of {table_name}: {detail}"

# -------------------------------
# Get one SK_ID sample
# -------------------------------
def fetch_customer_data(sk_id):
    app_df = pd.read_sql(f"SELECT * FROM SAMPLE_APPLICATION WHERE SK_ID_CURR = {sk_id}", conn)
    bureau_df = pd.read_sql(f"SELECT * FROM SAMPLE_BUREAU WHERE SK_ID_CURR = {sk_id}", conn)
    return app_df, bureau_df

# -------------------------------
# Process one customer
# -------------------------------
def process_customer(sk_id):
    app_df, bureau_df = fetch_customer_data(sk_id)
    all_checks = []

    # --- Application table checks ---
    for col in ["AMT_ANNUITY", "AMT_CREDIT", "AMT_INCOME_TOTAL", "DAYS_EMPLOYED"]:
        vals = app_df[col].dropna()
        missing_pct = 1 - len(vals) / len(app_df) if len(app_df) else 0
        if missing_pct > 0:
            sev = classify_issue("MISSING", pct=missing_pct)
            all_checks.append(("SAMPLE_APPLICATION", col, "MISSING", sev, round(missing_pct*100,1)))
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
            all_checks.append(("SAMPLE_BUREAU", col, "MISSING", sev, round(missing_pct*100,1)))
        for v in vals:
            if v < 0:
                sev = classify_issue("NEGATIVE", value=v)
                all_checks.append(("SAMPLE_BUREAU", col, "NEGATIVE", sev, v))

    # -------------------------------
    # Build combined payload for LLM
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

        summary = make_summary(table_name, col, column_desc, issue_type, severity, detail)

        payload_checks.append({
            "table_name": table_name,
            "column_name": col,
            "column_desc": column_desc,
            "check_summary": summary
        })

    payload = {
        "customer_id": str(sk_id),
        "all_checks": payload_checks
    }

    print(f"\n📤 Sending combined payload to LLM:\n{json.dumps(payload, indent=2)}\n")
    try:
        response = requests.post("http://127.0.0.1:8000/analyze_combined", json=payload).json()

        insert_sql = """
            INSERT INTO DQ_AI_SUGGESTIONS
            (TABLE_NAME, COLUMN_NAME, ISSUE_DESCRIPTION, AI_SUGGESTION, CONFIDENCE_SCORE, TIMESTAMP)
            SELECT %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
        """
        for check in payload_checks:
            cur.execute(
                insert_sql,
                (
                    check["table_name"],
                    check["column_name"],
                    check["check_summary"],
                    json.dumps(response),
                    response.get("confidence", 0.0),
                ),
            )
        conn.commit()
        print(f"✅ Stored combined suggestion for customer {sk_id}, confidence={response.get('confidence','N/A')}")
    except Exception as e:
        print(f"❌ Error sending to LLM: {e}")

# -------------------------------
# MAIN
# -------------------------------
if __name__ == "__main__":
    test_sk_id = 171559  # pick one ID to test
    process_customer(test_sk_id)

    cur.close()
    conn.close()

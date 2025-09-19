import os
import json
import requests
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv("variables.env")

# -------------------------------
# Summarizer Function
# -------------------------------
def summarize_check(table_name, column_name, check_type, check_details_str):
    """
    Convert raw profiling JSON into a clean English summary for the LLM.
    """
    try:
        details = json.loads(check_details_str)
    except Exception:
        return f"Issue detected in {table_name}.{column_name}: {check_details_str}"

    if check_type == "anomaly_check":
        desc = details.get("description", "Anomaly detected")
        results = details.get("results", [{}])[0]
        parts = []
        if "MIN_VAL" in results and "MAX_VAL" in results:
            parts.append(f"Min={results['MIN_VAL']}, Max={results['MAX_VAL']}")
        if "AVG_VAL" in results:
            parts.append(f"Avg={results['AVG_VAL']:.2f}")
        if "MISSING_COUNT" in results:
            parts.append(f"Missing={results['MISSING_COUNT']}")
        stats = ", ".join(parts)
        return f"{desc}. Stats: {stats}"

    return details.get("description", f"Check issue in {table_name}.{column_name}")

# -------------------------------
# Snowflake Connection
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
# Fetch latest checks
# -------------------------------
cur.execute("""
    SELECT TABLE_NAME, COLUMN_NAME, CHECK_TYPE, CHECK_DETAILS
    FROM DQ_CHECKS
    ORDER BY TIMESTAMP DESC
    LIMIT 5
""")
rows = cur.fetchall()

# -------------------------------
# Process each check
# -------------------------------
for row in rows:
    table_name, column_name, check_type, check_details = row

    # Summarize
    summary = summarize_check(table_name, column_name, check_type, check_details)

    print(f"\n📤 Sending to LLM API:\n{summary}\n")

    try:
        # Send only summary to API
        response = requests.post("http://127.0.0.1:8000/analyze", json={"summary": summary}).json()
        suggestion = response.get("ai_suggestion", "")
        confidence = response.get("confidence_score", 0.0)

        # Insert into Snowflake
        insert_sql = """
            INSERT INTO DQ_AI_SUGGESTIONS (TABLE_NAME, COLUMN_NAME, ISSUE_DESCRIPTION, AI_SUGGESTION, CONFIDENCE_SCORE, TIMESTAMP)
            SELECT %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
        """
        cur.execute(insert_sql, (table_name, column_name, summary, suggestion, confidence))
        conn.commit()

        print(f"✅ Stored suggestion for {table_name}.{column_name}: {suggestion}")

    except Exception as e:
        print(f"❌ Error processing {table_name}.{column_name}: {e}")

cur.close()
conn.close()

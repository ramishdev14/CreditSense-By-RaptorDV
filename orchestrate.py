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
    Convert raw profiling JSON into a condensed English summary for the LLM.
    """
    try:
        details = json.loads(check_details_str)
    except Exception:
        return f"Issue detected in {table_name}.{column_name}: {check_details_str}"

    desc = details.get("description", f"Issue in {column_name}")
    results = details.get("results", [])

    stats_parts = []
    if isinstance(results, list) and results:
        res = results[0]
        if "MIN_VAL" in res and "MAX_VAL" in res:
            stats_parts.append(f"Range: {res['MIN_VAL']}–{res['MAX_VAL']}")
        if "AVG_VAL" in res:
            try:
                stats_parts.append(f"Average: {float(res['AVG_VAL']):.2f}")
            except Exception:
                stats_parts.append(f"Average: {res['AVG_VAL']}")
        if "MISSING_COUNT" in res:
            stats_parts.append(f"Missing: {res['MISSING_COUNT']}")
        if "CNT" in res:
            stats_parts.append(f"Rows checked: {res['CNT']}")

    stats_summary = ", ".join(stats_parts) if stats_parts else "No summary stats available"
    return f"{desc}. {stats_summary}"

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
# Fetch checks from all tables
# -------------------------------
check_tables = ["APP_CHECKS", "BUREAU_CHECKS", "PREV_APP_CHECKS", "INST_CHECKS"]

for check_table in check_tables:
    print(f"\n🔍 Processing checks from {check_table}...\n")

    cur.execute(f"""
        SELECT 
            c.TABLE_NAME, 
            c.COLUMN_NAME, 
            c.CHECK_TYPE, 
            c.CHECK_DETAILS, 
            d.DESCRIPTION
        FROM {check_table} AS c
        LEFT JOIN COLUMN_DICTIONARY AS d
            ON UPPER(c.TABLE_NAME) = UPPER(d.TABLE_NAME)
           AND UPPER(c.COLUMN_NAME) = UPPER(d.ROW_NAME)
        ORDER BY c.TIMESTAMP DESC
        LIMIT 5
    """)

    rows = cur.fetchall()

    for row in rows:
        table_name, column_name, check_type, check_details, column_desc = row
        column_desc = column_desc or "No description available"

        # Build summary for LLM
        summary = summarize_check(table_name, column_name, check_type, check_details)

        print(f"\n📤 Sending to LLM API for {table_name}.{column_name}...\n")

        try:
            # Send structured payload
            payload = {
                "table_name": table_name,
                "column_name": column_name,
                "column_desc": column_desc,
                "check_summary": summary
            }
            response = requests.post("http://127.0.0.1:8000/analyze", json=payload).json()

            # Store the raw JSON output from LLM
            insert_sql = """
                INSERT INTO DQ_AI_SUGGESTIONS 
                (TABLE_NAME, COLUMN_NAME, ISSUE_DESCRIPTION, AI_SUGGESTION, CONFIDENCE_SCORE, TIMESTAMP)
                SELECT %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
            """
            cur.execute(
                insert_sql,
                (
                    table_name,
                    column_name,
                    summary,
                    json.dumps(response),  # store full structured JSON
                    response.get("confidence", 0.0),
                ),
            )
            conn.commit()

            print(f"✅ Stored suggestion for {table_name}.{column_name}: {response.get('suggestion', 'N/A')}")

        except Exception as e:
            print(f"❌ Error processing {table_name}.{column_name}: {e}")

cur.close()
conn.close()

import snowflake.connector
import requests
import os
from dotenv import load_dotenv

# Load env vars
load_dotenv("variables.env")

# Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)
cur = conn.cursor()

# Step 1: Get profiling results (DQ_CHECKS)
cur.execute("SELECT CHECK_ID, TABLE_NAME, COLUMN_NAME, CHECK_TYPE, CHECK_DETAILS FROM DQ_CHECKS ORDER BY TIMESTAMP DESC LIMIT 5")
rows = cur.fetchall()

# Step 2: Send each row to the local API
for row in rows:
    check_id, table_name, column_name, check_type, check_details = row

    payload = {
        "table_name": table_name,
        "column_name": column_name,
        "check_type": check_type,
        "check_details": check_details
    }

    response = requests.post("http://localhost:8000/analyze", json=payload)
    if response.status_code != 200:
        print(f"❌ API failed for CHECK_ID={check_id}: {response.text}")
        continue

    result = response.json()

    # Step 3: Insert response into DQ_AI_SUGGESTIONS
    insert_sql = """
        INSERT INTO DQ_AI_SUGGESTIONS (TABLE_NAME, COLUMN_NAME, ISSUE_DESCRIPTION, AI_SUGGESTION, CONFIDENCE_SCORE, TIMESTAMP)
        SELECT %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
    """
    cur.execute(insert_sql, (
        table_name,
        column_name,
        result["issue_description"],
        result["ai_suggestion"],
        result["confidence_score"]
    ))

    print(f"✅ Inserted suggestion for CHECK_ID={check_id}")

conn.commit()
cur.close()
conn.close()

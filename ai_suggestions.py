import snowflake.connector
import os
from dotenv import load_dotenv

# Load env vars
load_dotenv("variables.env")

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)
cur = conn.cursor()

# Query latest AI suggestions
cur.execute("""
    SELECT TABLE_NAME, COLUMN_NAME, ISSUE_DESCRIPTION, AI_SUGGESTION, CONFIDENCE_SCORE, TIMESTAMP
    FROM DQ_AI_SUGGESTIONS
    ORDER BY TIMESTAMP DESC
    LIMIT 10
""")

rows = cur.fetchall()

print("\n🧠 Latest AI Suggestions:\n")
for row in rows:
    table_name, column_name, issue_desc, suggestion, confidence, ts = row
    print(f"📌 Table: {table_name}, Column: {column_name}")
    print(f"   📝 Issue: {issue_desc}")
    print(f"   💡 Suggestion: {suggestion}")
    print(f"   🎯 Confidence: {confidence:.2f}")
    print(f"   ⏰ Timestamp: {ts}\n")

cur.close()
conn.close()

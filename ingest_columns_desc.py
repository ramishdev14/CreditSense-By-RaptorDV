import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# Load env vars
load_dotenv("variables.env")

sf_account = os.getenv("SNOWFLAKE_ACCOUNT")
sf_user = os.getenv("SNOWFLAKE_USER")
sf_password = os.getenv("SNOWFLAKE_PASSWORD")
sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
sf_database = os.getenv("SNOWFLAKE_DATABASE")
sf_schema = os.getenv("SNOWFLAKE_SCHEMA")

# Path to the CSV
file_path = r"C:\Users\PE ERP Lab\Documents\HomeCredit_columns_description.csv"

# Read CSV with fallback encoding
try:
    df = pd.read_csv(file_path, encoding="utf-8")
except UnicodeDecodeError:
    print("⚠️ UTF-8 decoding failed, retrying with latin1 encoding...")
    df = pd.read_csv(file_path, encoding="latin1")

# Drop index-like columns if they exist
df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

# Rename columns to match Snowflake table schema
df = df.rename(columns={
    "Table": "TABLE_NAME",
    "Row": "ROW_NAME",
    "Description": "DESCRIPTION",
    "Special": "SPECIAL"
})

# Connect to Snowflake
ctx = snowflake.connector.connect(
    account=sf_account,
    user=sf_user,
    password=sf_password,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema
)

# Write to Snowflake
success, nchunks, nrows, _ = write_pandas(
    conn=ctx,
    df=df,
    table_name="RAW_COLUMN_DESCRIPTION",
    auto_create_table=False
)

print(f"Ingestion successful: {success}, rows: {nrows}")
ctx.close()

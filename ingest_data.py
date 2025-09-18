import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv  # Import the function to load .env file

# --- Load environment variables from .env file ---
load_dotenv('variables.env')  

# --- Load credentials from environment variables ---
sf_account = os.getenv('SNOWFLAKE_ACCOUNT')
sf_user = os.getenv('SNOWFLAKE_USER')
sf_password = os.getenv('SNOWFLAKE_PASSWORD')
sf_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
sf_database = os.getenv('SNOWFLAKE_DATABASE')
sf_schema = os.getenv('SNOWFLAKE_SCHEMA')

# Check if credentials are loaded
if not all([sf_account, sf_user, sf_password]):
    raise ValueError("Failed to load Snowflake credentials. Please check your variables.env file.")

print("Successfully loaded environment variables.")
print(f"Connecting to: {sf_account} | Database: {sf_database}.{sf_schema}")

# 1. Read only the shortlisted columns from the CSV file
file_path = r'D:\Ramish\home-credit-default-risk\application_train.csv'
print(f"Reading data from {file_path}...")

# SHORTLISTED COLUMNS FOR THE HACKATHON PROTOTYPE
cols_to_use = [
    'SK_ID_CURR', 
    'TARGET', 
    'NAME_CONTRACT_TYPE', 
    'CODE_GENDER',
    'FLAG_OWN_CAR', 
    'FLAG_OWN_REALTY', 
    'CNT_CHILDREN',
    'AMT_INCOME_TOTAL', 
    'AMT_CREDIT', 
    'AMT_ANNUITY',
    'DAYS_BIRTH', 
    'DAYS_EMPLOYED', 
    'OCCUPATION_TYPE',
    'ORGANIZATION_TYPE', 
    'FLAG_WORK_PHONE', 
    'FLAG_PHONE', 
    'FLAG_EMAIL'
]

print(f"Selected {len(cols_to_use)} key columns for the prototype.")
df = pd.read_csv(file_path, usecols=cols_to_use)
print(f"DataFrame shape: {df.shape}")

# 2. Connect to Snowflake
print("Connecting to Snowflake...")
ctx = snowflake.connector.connect(
    account=sf_account,
    user=sf_user,
    password=sf_password,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema
)

# 3. Write the data to the table
print("Writing data to Snowflake table 'RAW_APPLICATION_DATA'...")
success, nchunks, nrows, _ = write_pandas(
    conn=ctx,
    df=df,
    table_name='RAW_APPLICATION_DATA',
    auto_create_table=False # We created the table manually, so set to False
)

if success:
    print(f"Ingestion successful! Uploaded {nrows} rows.")
else:
    print("Ingestion failed.")

# 4. Close the connection
ctx.close()
print("Done.")
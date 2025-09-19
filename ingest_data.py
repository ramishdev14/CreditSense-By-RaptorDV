import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# --- Load environment variables from .env file ---
load_dotenv("variables.env")

# --- Load credentials from environment variables ---
sf_account = os.getenv("SNOWFLAKE_ACCOUNT")
sf_user = os.getenv("SNOWFLAKE_USER")
sf_password = os.getenv("SNOWFLAKE_PASSWORD")
sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
sf_database = os.getenv("SNOWFLAKE_DATABASE")
sf_schema = os.getenv("SNOWFLAKE_SCHEMA")

if not all([sf_account, sf_user, sf_password]):
    raise ValueError("❌ Failed to load Snowflake credentials. Please check variables.env")

print("✅ Successfully loaded environment variables.")
print(f"Connecting to: {sf_account} | Database: {sf_database}.{sf_schema}")

# --------------------------------
# Helper function for ingestion
# --------------------------------
def ingest_csv_to_snowflake(csv_path, table_name, cols_to_use):
    print(f"\n📂 Reading {csv_path}...")
    df = pd.read_csv(csv_path, usecols=cols_to_use)
    print(f"DataFrame shape: {df.shape}")

    # Connect to Snowflake
    ctx = snowflake.connector.connect(
        account=sf_account,
        user=sf_user,
        password=sf_password,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema,
    )

    print(f"⬆️ Uploading data to {table_name}...")
    success, nchunks, nrows, _ = write_pandas(
        conn=ctx, df=df, table_name=table_name, auto_create_table=False
    )

    if success:
        print(f"✅ Ingestion successful! Uploaded {nrows} rows into {table_name}.")
    else:
        print(f"❌ Ingestion failed for {table_name}.")

    ctx.close()

'''
# --------------------------------
# 1. Application Train
# --------------------------------
ingest_csv_to_snowflake(
    r"D:\Ramish\home-credit-default-risk\application_train.csv",
    "RAW_APPLICATION_DATA",
    [
        "SK_ID_CURR",
        "TARGET",
        "NAME_CONTRACT_TYPE",
        "CODE_GENDER",
        "FLAG_OWN_CAR",
        "FLAG_OWN_REALTY",
        "CNT_CHILDREN",
        "AMT_INCOME_TOTAL",
        "AMT_CREDIT",
        "AMT_ANNUITY",
        "DAYS_BIRTH",
        "DAYS_EMPLOYED",
        "OCCUPATION_TYPE",
        "ORGANIZATION_TYPE",
        "FLAG_WORK_PHONE",
        "FLAG_PHONE",
        "FLAG_EMAIL",
    ],
)
'''
# --------------------------------
# 2. Bureau
# --------------------------------
ingest_csv_to_snowflake(
    r"C:\Users\PE ERP Lab\Documents\bureau\bureau.csv",
    "RAW_BUREAU",
    [
        "SK_ID_CURR",
        "SK_ID_BUREAU",
        "CREDIT_ACTIVE",
        "CREDIT_TYPE",
        "DAYS_CREDIT",
        "CREDIT_DAY_OVERDUE",
        "DAYS_CREDIT_ENDDATE",
        "DAYS_ENDDATE_FACT",
        "AMT_CREDIT_MAX_OVERDUE",
        "CNT_CREDIT_PROLONG",
        "AMT_CREDIT_SUM",
        "AMT_CREDIT_SUM_DEBT",
        "AMT_CREDIT_SUM_LIMIT",
        "AMT_CREDIT_SUM_OVERDUE",
        "DAYS_CREDIT_UPDATE",
        "AMT_ANNUITY",
    ],
)

# --------------------------------
# 3. Previous Application
# --------------------------------
ingest_csv_to_snowflake(
    r"C:\Users\PE ERP Lab\Documents\previous_application\previous_application.csv",
    "RAW_PREVIOUS_APP",
    [
        "SK_ID_PREV",
        "SK_ID_CURR",
        "NAME_CONTRACT_TYPE",
        "AMT_ANNUITY",
        "AMT_APPLICATION",
        "AMT_CREDIT",
        "AMT_DOWN_PAYMENT",
        "AMT_GOODS_PRICE",
        "CNT_PAYMENT",
        "NAME_CONTRACT_STATUS",
        "NAME_PORTFOLIO",
        "CHANNEL_TYPE",
        "PRODUCT_COMBINATION",
        "DAYS_DECISION",
        "DAYS_FIRST_DRAWING",
        "DAYS_FIRST_DUE",
        "DAYS_LAST_DUE",
        "DAYS_TERMINATION",
    ],
)

# --------------------------------
# 4. Installments Payments
# --------------------------------
ingest_csv_to_snowflake(
    r"C:\Users\PE ERP Lab\Documents\installments_payments\installments_payments.csv",
    "RAW_INSTALLMENTS",
    [
        "SK_ID_PREV",
        "SK_ID_CURR",
        "NUM_INSTALMENT_NUMBER",
        "DAYS_INSTALMENT",
        "DAYS_ENTRY_PAYMENT",
        "AMT_INSTALMENT",
        "AMT_PAYMENT",
    ],
)

print("\n🎉 All ingestions completed!")

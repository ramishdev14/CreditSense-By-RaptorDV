# dq_profiler_api.py
from fastapi import FastAPI, Query
from enum import Enum
from typing import List, Optional, Dict, Any
import os
import json
from datetime import datetime

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# ------------------------------------------------------
# Env & Snowflake connection
# ------------------------------------------------------
load_dotenv("variables.env")

SF_CFG = dict(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

TABLES = {
    "SAMPLE_APPLICATION": {"key_curr": "SK_ID_CURR", "key_prev": None, "checks_table": "APP_CHECKS"},
    "SAMPLE_BUREAU": {"key_curr": "SK_ID_CURR", "key_prev": None, "checks_table": "BUREAU_CHECKS"},
    "SAMPLE_PREVIOUS_APP": {"key_curr": "SK_ID_CURR", "key_prev": "SK_ID_PREV", "checks_table": "PREV_APP_CHECKS"},
    "SAMPLE_INSTALLMENTS": {"key_curr": "SK_ID_CURR", "key_prev": "SK_ID_PREV", "checks_table": "INST_CHECKS"},
}

app = FastAPI(title="DQ Row-Level Profiler", version="1.0")

def get_conn():
    return snowflake.connector.connect(**SF_CFG)

# ------------------------------------------------------
# INSERT helper — uses existing checks schema:
# (TABLE_NAME, COLUMN_NAME, CHECK_TYPE, CHECK_DETAILS VARIANT, SK_ID_CURR, SK_ID_PREV, TIMESTAMP)
# ------------------------------------------------------
def insert_issue(cur, checks_table: str, table_name: str,
                 column_name: str, issue_type: str, issue_value: Any,
                 sk_id_curr: Optional[Any] = None,
                 sk_id_prev: Optional[Any] = None,
                 sk_id_bureau: Optional[Any] = None):
    details = {"value": issue_value}

    if checks_table == "APP_CHECKS":
        cur.execute(
            f"""
            INSERT INTO {checks_table}
            (TABLE_NAME, COLUMN_NAME, CHECK_TYPE, CHECK_DETAILS, SK_ID_CURR, TIMESTAMP)
            SELECT %s, %s, %s, PARSE_JSON(%s), %s, CURRENT_TIMESTAMP
            """,
            (table_name, column_name, issue_type, json.dumps(details), sk_id_curr)
        )

    elif checks_table == "BUREAU_CHECKS":
        cur.execute(
            f"""
            INSERT INTO {checks_table}
            (TABLE_NAME, COLUMN_NAME, CHECK_TYPE, CHECK_DETAILS, SK_ID_CURR, SK_ID_BUREAU, TIMESTAMP)
            SELECT %s, %s, %s, PARSE_JSON(%s), %s, %s, CURRENT_TIMESTAMP
            """,
            (table_name, column_name, issue_type, json.dumps(details), sk_id_curr, sk_id_bureau)
        )

    else:  # PREV_APP_CHECKS and INST_CHECKS
        cur.execute(
            f"""
            INSERT INTO {checks_table}
            (TABLE_NAME, COLUMN_NAME, CHECK_TYPE, CHECK_DETAILS, SK_ID_CURR, SK_ID_PREV, TIMESTAMP)
            SELECT %s, %s, %s, PARSE_JSON(%s), %s, %s, CURRENT_TIMESTAMP
            """,
            (table_name, column_name, issue_type, json.dumps(details), sk_id_curr, sk_id_prev)
        )

# ------------------------------------------------------
# Rule packs (row-level)
# ------------------------------------------------------
VALID_CODE_GENDER = {"M", "F"}  # dataset also has 'XNA' occasionally — treat as invalid
VALID_CONTRACT_TYPE = {"Cash loans", "Revolving loans"}
VALID_CREDIT_ACTIVE = {"Active", "Bad debt", "Closed", "Sold"}
VALID_CREDIT_CURRENCY = {"currency 1", "currency 2", "currency 3", "currency 4"}

def is_number(x):
    return isinstance(x, (int, float)) and pd.notna(x)

def rulepack_application(row: pd.Series, cur, checks_table: str):
    sk_curr = row.get("SK_ID_CURR")

    # Nulls
    for col, val in row.items():
        if col == "SK_ID_CURR":
            continue
        if pd.isna(val):
            insert_issue(cur, checks_table, "SAMPLE_APPLICATION", col,
                         "NULL_VALUE", None, sk_id_curr=sk_curr)

    # Negative values
    for col in ["CNT_CHILDREN", "AMT_INCOME_TOTAL", "AMT_CREDIT", "AMT_ANNUITY"]:
        val = row.get(col)
        if is_number(val) and val < 0:
            insert_issue(cur, checks_table, "SAMPLE_APPLICATION", col,
                         "NEGATIVE_VALUE", val, sk_id_curr=sk_curr)

    # Days anomalies
    if is_number(row.get("DAYS_BIRTH")) and row["DAYS_BIRTH"] > 0:
        insert_issue(cur, checks_table, "SAMPLE_APPLICATION", "DAYS_BIRTH",
                     "INVALID_AGE_DIRECTION", row["DAYS_BIRTH"], sk_id_curr=sk_curr)

    if is_number(row.get("DAYS_EMPLOYED")) and row["DAYS_EMPLOYED"] > 0:
        insert_issue(cur, checks_table, "SAMPLE_APPLICATION", "DAYS_EMPLOYED",
                     "INVALID_EMPLOYMENT_DAYS", row["DAYS_EMPLOYED"], sk_id_curr=sk_curr)


def rulepack_bureau(row: pd.Series, cur, checks_table: str):
    sk_curr = row.get("SK_ID_CURR")
    sk_bureau = row.get("SK_ID_BUREAU")

    for col, val in row.items():
        if col in ("SK_ID_CURR", "SK_ID_BUREAU"):
            continue
        if pd.isna(val):
            insert_issue(cur, checks_table, "SAMPLE_BUREAU", col,
                         "NULL_VALUE", None, sk_id_curr=sk_curr, sk_id_bureau=sk_bureau)

    # Negative amounts
    for col in ["AMT_CREDIT_MAX_OVERDUE", "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT",
                "AMT_CREDIT_SUM_LIMIT", "AMT_CREDIT_SUM_OVERDUE", "AMT_ANNUITY"]:
        val = row.get(col)
        if is_number(val) and val < 0:
            insert_issue(cur, checks_table, "SAMPLE_BUREAU", col,
                         "NEGATIVE_VALUE", val, sk_id_curr=sk_curr, sk_id_bureau=sk_bureau)


def rulepack_previous_app(row: pd.Series, cur, checks_table: str):
    sk_curr = row.get("SK_ID_CURR")
    sk_prev = row.get("SK_ID_PREV")

    for col, val in row.items():
        if col in ("SK_ID_CURR", "SK_ID_PREV"):
            continue
        if pd.isna(val):
            insert_issue(cur, checks_table, "SAMPLE_PREVIOUS_APP", col,
                         "NULL_VALUE", None, sk_id_curr=sk_curr, sk_id_prev=sk_prev)

    # Negative amounts
    for col in ["AMT_ANNUITY", "AMT_APPLICATION", "AMT_CREDIT", "AMT_DOWN_PAYMENT", "AMT_GOODS_PRICE"]:
        val = row.get(col)
        if is_number(val) and val < 0:
            insert_issue(cur, checks_table, "SAMPLE_PREVIOUS_APP", col,
                         "NEGATIVE_VALUE", val, sk_id_curr=sk_curr, sk_id_prev=sk_prev)


def rulepack_installments(row: pd.Series, cur, checks_table: str):
    sk_curr = row.get("SK_ID_CURR")
    sk_prev = row.get("SK_ID_PREV")

    for col, val in row.items():
        if col in ("SK_ID_CURR", "SK_ID_PREV"):
            continue
        if pd.isna(val):
            insert_issue(cur, checks_table, "SAMPLE_INSTALLMENTS", col,
                         "NULL_VALUE", None, sk_id_curr=sk_curr, sk_id_prev=sk_prev)

    inst = row.get("AMT_INSTALMENT")
    pay = row.get("AMT_PAYMENT")
    if is_number(inst) and is_number(pay):
        if pay > inst * 1.5:
            insert_issue(cur, checks_table, "SAMPLE_INSTALLMENTS", "AMT_PAYMENT",
                         "POTENTIAL_OVERPAYMENT", pay, sk_id_curr=sk_curr, sk_id_prev=sk_prev)
        if pay < 0.5 * inst:
            insert_issue(cur, checks_table, "SAMPLE_INSTALLMENTS", "AMT_PAYMENT",
                         "POTENTIAL_UNDERPAYMENT", pay, sk_id_curr=sk_curr, sk_id_prev=sk_prev)

# Map table → rulepack
RULEPACKS = {
    "SAMPLE_APPLICATION": rulepack_application,
    "SAMPLE_BUREAU": rulepack_bureau,
    "SAMPLE_PREVIOUS_APP": rulepack_previous_app,
    "SAMPLE_INSTALLMENTS": rulepack_installments,
}

# ------------------------------------------------------
# Core profiler
# ------------------------------------------------------
def profile_table(conn, table_name: str):
    cfg = TABLES[table_name]
    checks_table = cfg["checks_table"]
    key_curr = cfg["key_curr"]; key_prev = cfg["key_prev"]

    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    if df.empty:
        return {"table": table_name, "rows": 0, "issues": 0}

    cur = conn.cursor()
    issues_before = cur.execute(f"SELECT COUNT(*) FROM {checks_table}").fetchone()[0]

    # Duplicate check by key(s)
    keys = [k for k in [key_curr, key_prev] if k]
    if keys:
        dup = df.duplicated(subset=keys, keep=False)
        for _, r in df[dup].iterrows():
            insert_issue(cur, checks_table, table_name, "|".join(keys), "DUPLICATE_KEY",
                         "|".join(str(r[k]) for k in keys),
                         r.get(key_curr), r.get(key_prev))

    # Row-level rules
    rulepack = RULEPACKS[table_name]
    for _, row in df.iterrows():
        rulepack(row, cur, checks_table)   # ✅ pass cur explicitly

    conn.commit()
    issues_after = cur.execute(f"SELECT COUNT(*) FROM {checks_table}").fetchone()[0]
    cur.close()
    return {"table": table_name, "rows": int(df.shape[0]), "issues": int(issues_after - issues_before)}

# ------------------------------------------------------
# Endpoints
# ------------------------------------------------------
class ProfileTarget(str, Enum):
    SAMPLE_APPLICATION = "SAMPLE_APPLICATION"
    SAMPLE_BUREAU = "SAMPLE_BUREAU"
    SAMPLE_PREVIOUS_APP = "SAMPLE_PREVIOUS_APP"
    SAMPLE_INSTALLMENTS = "SAMPLE_INSTALLMENTS"

@app.get("/profile_all")
def profile_all(truncate_first: bool = True):
    conn = get_conn()
    cur = conn.cursor()

    if truncate_first:
        print("🧹 Truncating all check tables...")
        cur.execute("TRUNCATE TABLE APP_CHECKS")
        cur.execute("TRUNCATE TABLE BUREAU_CHECKS")
        cur.execute("TRUNCATE TABLE PREV_APP_CHECKS")
        cur.execute("TRUNCATE TABLE INST_CHECKS")
        conn.commit()

    cur.close()

    results = []
    # Explicit calls, easier to debug/manage
    results.append(profile_table(conn, "SAMPLE_APPLICATION"))
    results.append(profile_table(conn, "SAMPLE_BUREAU"))
    results.append(profile_table(conn, "SAMPLE_PREVIOUS_APP"))
    results.append(profile_table(conn, "SAMPLE_INSTALLMENTS"))

    conn.close()
    return {"status": "ok", "results": results}

import streamlit as st
import pandas as pd
import snowflake.connector
import os
import requests
from dotenv import load_dotenv

# Import formatter for business-friendly display
from formatter import display_ai_suggestion

# -------------------------------
# Load env vars & connect to Snowflake
# -------------------------------
load_dotenv("variables.env")

ORCHESTRATOR_API_URL = os.getenv(
    "ORCHESTRATOR_API_URL", "http://orchestrator-api:8002"
)

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

# -------------------------------
# Streamlit Dashboard
# -------------------------------
st.set_page_config(page_title="DQ Dashboard", layout="wide")

st.title("📊 Data Quality Monitoring Dashboard")

# Customer input
customer_id = st.text_input("Enter Customer ID (SK_ID_CURR):", "")

if st.button("Process Results"):
    if not customer_id.strip():
        st.warning("⚠️ Please enter a valid Customer ID.")
    else:
        try:
            with st.spinner("⏳ Processing customer data and fetching AI suggestions..."):
                # ✅ Call orchestrator-api instead of local process_customer
                resp = requests.post(
                    f"{ORCHESTRATOR_API_URL}/process_customer",
                    json={"sk_id": int(customer_id)},
                )
                resp.raise_for_status()
                result = resp.json()

                # Fetch anomalies
                anomalies_query = f"""
                    SELECT *
                    FROM DQ_ANOMALIES
                    WHERE SK_ID_CURR = {customer_id}
                    ORDER BY TIMESTAMP DESC
                    LIMIT 10
                """
                anomalies_df = pd.read_sql(anomalies_query, conn)

                # Fetch AI suggestions
                suggestions_query = f"""
                    SELECT *
                    FROM DQ_AI_SUGGESTIONS
                    WHERE SK_ID_CURR = {customer_id}
                    ORDER BY TIMESTAMP DESC
                    LIMIT 10
                """
                suggestions_df = pd.read_sql(suggestions_query, conn)

            # -------------------------------
            # Refresh Banner
            # -------------------------------
            st.success(
                f"✅ Processing complete! Results for Customer {customer_id} were refreshed."
            )

            # -------------------------------
            # Summary Banner (Business Snapshot)
            # -------------------------------
            if anomalies_df.empty:
                st.info(f"✅ No anomalies detected for customer {customer_id}.")
            else:
                total_anomalies = len(anomalies_df)
                high_count = sum(
                    1 for x in anomalies_df["ANOMALY_DETAILS"].astype(str).values if "High" in x
                )
                med_count = sum(
                    1 for x in anomalies_df["ANOMALY_DETAILS"].astype(str).values if "Medium" in x
                )
                low_count = total_anomalies - high_count - med_count

                st.markdown(
                    f"""
                    ### 🚨 Customer {customer_id} Summary
                    - **Total anomalies:** {total_anomalies}  
                    - 🔴 High severity: **{high_count}**  
                    - 🟠 Medium severity: **{med_count}**  
                    - 🟢 Low severity: **{low_count}**
                    """
                )

            # -------------------------------
            # Detailed Results
            # -------------------------------
            st.subheader("Detected Anomalies (Raw View)")
            if anomalies_df.empty:
                st.info("No anomalies found for this customer.")
            else:
                st.dataframe(anomalies_df, width=1200)

            st.subheader("AI Suggestions (Business-Friendly View)")
            if suggestions_df.empty:
                st.info("No AI suggestions available for this customer.")
            else:
                for _, row in suggestions_df.iterrows():
                    display_ai_suggestion(row)
                    st.divider()

            with st.expander("🔍 Show Raw AI Suggestions Table"):
                st.dataframe(suggestions_df, width=1200)

        except Exception as e:
            st.error(f"❌ Error: {e}")

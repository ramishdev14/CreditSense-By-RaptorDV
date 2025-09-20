import streamlit as st
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

# Import orchestrator function
from orchestrate import process_customer

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
                # Run orchestration
                process_customer(int(customer_id))

                # Fetch anomalies
                anomalies_query = f"""
                    SELECT *
                    FROM DQ_ANOMALIES
                    WHERE SK_ID_CURR = {customer_id}
                    ORDER BY TIMESTAMP DESC
                    LIMIT 10
                """
                anomalies_df = pd.read_sql(anomalies_query, conn)

                # Fetch AI suggestions (now filtered by SK_ID_CURR directly)
                suggestions_query = f"""
                    SELECT *
                    FROM DQ_AI_SUGGESTIONS
                    WHERE SK_ID_CURR = {customer_id}
                    ORDER BY TIMESTAMP DESC
                    LIMIT 10
                """
                suggestions_df = pd.read_sql(suggestions_query, conn)

            # Show results after processing
            st.success("✅ Processing complete!")

            st.subheader("Detected Anomalies")
            if anomalies_df.empty:
                st.info("No anomalies found for this customer.")
            else:
                st.dataframe(anomalies_df, width=1200)

            st.subheader("AI Suggestions")
            if suggestions_df.empty:
                st.info("No AI suggestions available for this customer.")
            else:
                st.dataframe(suggestions_df, width=1200)

        except Exception as e:
            st.error(f"❌ Error: {e}")

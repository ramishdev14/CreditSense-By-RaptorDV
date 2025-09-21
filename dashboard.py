import streamlit as st
import pandas as pd
import snowflake.connector
import os
import requests
import json
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
# App state (session)
# -------------------------------
if "page" not in st.session_state:
    st.session_state.page = "front"  # default to front page
if "selected_id" not in st.session_state:
    st.session_state.selected_id = None

# -------------------------------
# Front Page
# -------------------------------
if st.session_state.page == "front":
    st.set_page_config(page_title="CreditSense", layout="centered")
    st.title("💳 CreditSense")
    st.markdown(
        """
        ### Unlocking Data Quality with AI
        **CreditSense** is an AI-powered data quality monitoring tool designed for the financial sector.  
        
        - ✅ Detects anomalies in customer credit data  
        - 🤖 Provides AI-driven root cause analysis  
        - 📈 Highlights business impact and actionable recommendations  
        
        This demo uses **realistic synthetic data** to showcase anomaly detection and AI insights.
        """
    )

    if st.button("🚀 Proceed to Dashboard"):
        st.session_state.page = "dashboard"
        st.rerun()


# -------------------------------
# Dashboard Page
# -------------------------------
elif st.session_state.page == "dashboard":
    st.set_page_config(page_title="CreditSense Dashboard", layout="wide")
    st.title("📊 CreditSense Dashboard")

    # Sample Customer IDs
    st.markdown("### 🔑 Try with Sample Customer IDs")
    sample_ids = [103065, 108032, 111950, 112961, 121072, 103788]
    cols = st.columns(len(sample_ids))
    for i, cid in enumerate(sample_ids):
        if cols[i].button(str(cid)):
            st.session_state.selected_id = cid

    # Customer ID input (pre-filled if chosen)
    customer_id = st.text_input(
        "Enter Customer ID (SK_ID_CURR):",
        value=st.session_state.selected_id if st.session_state.selected_id else "",
    )

    if st.button("Process Results"):
        if not customer_id.strip():
            st.warning("⚠️ Please enter a valid Customer ID.")
        else:
            try:
                with st.spinner("⏳ Processing customer data and fetching AI suggestions..."):
                    # ✅ Call orchestrator-api
                    resp = requests.post(
                        f"{ORCHESTRATOR_API_URL}/process_customer",
                        json={"sk_id": int(customer_id)},
                    )
                    resp.raise_for_status()

                    # Fetch anomalies
                    anomalies_query = f"""
                        SELECT *
                        FROM DQ_ANOMALIES
                        WHERE SK_ID_CURR = {customer_id}
                        ORDER BY TIMESTAMP DESC
                        LIMIT 50
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
                # Business Snapshot
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

                    # Show 3 latest AI suggestions (business-friendly)
                    if not suggestions_df.empty:
                        st.markdown("### 💡 Latest AI Suggestions")
                        for _, row in suggestions_df.head(3).iterrows():
                            try:
                                suggestion = json.loads(row["AI_SUGGESTION"])
                                sev = suggestion.get("severity", "low").lower()
                                icon = "🔴" if sev == "high" else ("🟠" if sev == "medium" else "🟢")
                                st.markdown(f"- {icon} **{suggestion.get('suggestion', 'No suggestion')}**")
                            except Exception:
                                st.markdown("- (Could not parse suggestion)")

                # -------------------------------
                # Business Impact Section
                # -------------------------------
                st.subheader("📈 Business Impact Assessment")
                if suggestions_df.empty:
                    st.info("No AI-based business impact assessment available.")
                else:
                    latest = None
                    try:
                        latest = json.loads(suggestions_df.iloc[0]["AI_SUGGESTION"])
                    except Exception:
                        pass

                    if latest:
                        severity = latest.get("severity", "Unknown").capitalize()
                        root_cause = latest.get("root_cause_hypothesis", "No root cause identified")
                        action = latest.get("suggestion", "No actionable recommendation")

                        severity_icon = "🔴" if severity.lower() == "high" else (
                            "🟠" if severity.lower() == "medium" else "🟢"
                        )

                        st.markdown(
                            f"""
                            - **Overall Data Risk Level:** {severity_icon} {severity}  
                            - **Likely Root Cause:** {root_cause}  
                            - **Recommended Action:** {action}
                            """
                        )
                    else:
                        st.info("Could not parse latest AI suggestion for business impact.")

                # -------------------------------
                # AI Suggestions (Detailed)
                # -------------------------------
                st.subheader("AI Suggestions (Business-Friendly View)")
                if suggestions_df.empty:
                    st.info("No AI suggestions available for this customer.")
                else:
                    for _, row in suggestions_df.iterrows():
                        display_ai_suggestion(row)
                        st.divider()

                # -------------------------------
                # Raw Tables (Hidden in Expanders)
                # -------------------------------
                with st.expander("📋 Show Raw Anomalies Table"):
                    st.dataframe(anomalies_df, width=1200)

                with st.expander("📋 Show Raw AI Suggestions Table"):
                    st.dataframe(suggestions_df, width=1200)

            except Exception as e:
                st.error(f"❌ Error: {e}")

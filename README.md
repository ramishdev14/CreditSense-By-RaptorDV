# 💳 CreditSense – AI-Powered Data Quality Monitoring

**CreditSense** is an AI-driven **data quality monitoring platform** designed for the financial domain.  
It detects anomalies in customer credit data, leverages LLMs for root cause analysis, and generates actionable AI-powered suggestions for data stewards.

🚀 Deployed on **AWS EKS**, it provides a scalable, cloud-native solution accessible via a web dashboard.

---

## 🌍 Live Demo

🔗 **Public Dashboard URL**: [https://teams.microsoft.com/l/message/19:75cb0a2d-ce90-45f9-9676-063a1a5db1e8_fe69d683-521c-4484-ae3d-cb5ef33c0c5b@unq.gbl.spaces/1758420068148?context=%7B%22contextType%22%3A%22chat%22%7D] 


---

## 📊 What It Does

- **Anomaly Detection**  
  - Checks for missing values, negatives, duplicates, and outliers in customer credit data.
  - Stores anomalies in **Snowflake**.

- **AI Suggestions (LLM Integration)**  
  - Uses **Google Gemini** to generate structured root cause hypotheses and data lineage mappings.
  - Stores AI suggestions in Snowflake for traceability.

- **Interactive Dashboard (Streamlit)**  
  - Enter a `Customer ID` to analyze results.
  - View anomalies and AI suggestions with clear **business-friendly formatting**.
  - Get a **business impact summary** and insights directly from the data.
  - Explore raw tables (hidden inside expanders).

- **Dataset Used**  
  - Sample credit risk data based on **Home Credit Dataset** (SAMPLE_APPLICATION, SAMPLE_BUREAU, etc.) preloaded in Snowflake.

---

## 🏗️ System Architecture


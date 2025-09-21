# 💳 CreditSense – AI-Powered Data Quality Monitoring

**CreditSense** is an AI-driven **data quality monitoring platform** designed for the financial domain.  
It detects anomalies in customer credit data, leverages LLMs for root cause analysis, and generates actionable AI-powered suggestions for data stewards.

🚀 Deployed on **AWS EKS**, it provides a scalable, cloud-native solution accessible via a web dashboard.

---

## 🌍 Live Demo

🔗 **Public Dashboard URL**: [http://<YOUR-EXTERNAL-IP>](http://<YOUR-EXTERNAL-IP>)  
*(Replace `<YOUR-EXTERNAL-IP>` with the actual LoadBalancer URL from `kubectl get svc dq-dashboard -n dq-ai`)*

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


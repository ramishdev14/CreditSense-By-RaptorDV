import pandas as pd
import numpy as np
import snowflake.connector
from dotenv import load_dotenv
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import joblib

# -----------------------------------
# Load env + Snowflake connection
# -----------------------------------
load_dotenv("variables.env")
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

# -----------------------------------
# Utility: compute profiling metrics
# -----------------------------------
def compute_features(df, key_cols):
    features = []
    for col in df.columns:
        if col in key_cols:
            continue
        col_data = df[col]

        # missing %
        missing_pct = col_data.isna().mean()

        # duplicates rate
        dup_rate = df.duplicated(subset=key_cols).mean()

        # outlier rate (zscore)
        if pd.api.types.is_numeric_dtype(col_data):
            mean, std = col_data.mean(), col_data.std()
            if std > 0:
                zscore = (col_data - mean) / std
                zscore_outlier_pct = ((np.abs(zscore) > 3).mean())
            else:
                zscore_outlier_pct = 0
        else:
            zscore_outlier_pct = 0

        features.append({
            "column": col,
            "missing_pct": missing_pct,
            "dup_rate": dup_rate,
            "zscore_outlier_pct": zscore_outlier_pct
        })
    return pd.DataFrame(features)

# -----------------------------------
# Fetch data from SAMPLE tables
# -----------------------------------
app_df = pd.read_sql("SELECT * FROM SAMPLE_APPLICATION", conn)
bureau_df = pd.read_sql("SELECT * FROM SAMPLE_BUREAU", conn)

# compute features
app_feats = compute_features(app_df, key_cols=["SK_ID_CURR"])
bureau_feats = compute_features(bureau_df, key_cols=["SK_ID_CURR"])

all_feats = pd.concat([app_feats, bureau_feats], ignore_index=True)

# -----------------------------------
# Auto-generate labels (bootstrap)
# -----------------------------------
def assign_severity(row):
    if row["missing_pct"] > 0.4 or row["zscore_outlier_pct"] > 0.2:
        return "High"
    elif row["missing_pct"] > 0.1 or row["zscore_outlier_pct"] > 0.05:
        return "Medium"
    else:
        return "Low"

all_feats["severity"] = all_feats.apply(assign_severity, axis=1)

# -----------------------------------
# Train classifier
# -----------------------------------
X = all_feats[["missing_pct", "dup_rate", "zscore_outlier_pct"]]
y = all_feats["severity"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_train, y_train)

print("Classification Report:")
print(classification_report(y_test, clf.predict(X_test)))

# save model
joblib.dump(clf, "dq_severity_model.pkl")
print("✅ Model saved as dq_severity_model.pkl")

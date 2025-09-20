import json
import streamlit as st

def format_ai_suggestion(row):
    """
    Takes a row from DQ_AI_SUGGESTIONS (pandas DataFrame) and
    returns a clean business-friendly dictionary for display.
    """

    try:
        suggestion = json.loads(row["AI_SUGGESTION"])
        if isinstance(suggestion, list):
            suggestion = suggestion[0]  # take first if multiple
    except Exception:
        return {"summary": "❌ Could not parse suggestion", "severity": "Unknown", "confidence": 0.0}

    severity = suggestion.get("severity", "Unknown").capitalize()
    confidence = suggestion.get("confidence", 0.0)
    dq_dim = suggestion.get("dq_dimension", "Unknown")
    business_fix = suggestion.get("suggestion", "No fix provided")
    rationale = suggestion.get("rationale", "No reasoning provided")
    root_cause = suggestion.get("root_cause_hypothesis", "Not available")

    lineage = suggestion.get("lineage_hypothesis", [])
    lineage_table = [
        f"{link.get('from_table')} ➝ {link.get('to_table')} ({link.get('reason')})"
        for link in lineage
    ]

    # Format severity with color tags
    if severity.lower() == "high":
        sev_tag = "🔴 High"
    elif severity.lower() == "medium":
        sev_tag = "🟡 Medium"
    elif severity.lower() == "low":
        sev_tag = "🟢 Low"
    else:
        sev_tag = severity

    return {
        "dimension": dq_dim,
        "severity": sev_tag,
        "confidence": f"{confidence*100:.1f}%",
        "business_fix": business_fix,
        "why": rationale,
        "root_cause": root_cause,
        "lineage": lineage_table
    }


def display_ai_suggestion(row):
    """
    Streamlit component to display one AI suggestion in a clean card.
    """
    formatted = format_ai_suggestion(row)

    st.markdown(f"### 📌 {formatted['dimension']} Issue")
    st.markdown(f"**Severity:** {formatted['severity']} | **Confidence:** {formatted['confidence']}")
    st.markdown(f"**Suggested Fix:** {formatted['business_fix']}")
    st.markdown(f"**Reasoning:** {formatted['why']}")
    st.markdown(f"**Root Cause Hypothesis:** {formatted['root_cause']}")

    if formatted["lineage"]:
        st.markdown("**Lineage Impact:**")
        for link in formatted["lineage"]:
            st.markdown(f"- {link}")

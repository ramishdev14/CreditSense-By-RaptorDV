import pandas as pd
import numpy as np

# --- CONFIGURATION ---
# Replace this with the actual path to your downloaded CSV file
csv_file_path = r'D:\Ramish\home-credit-default-risk\application_train.csv'
# ---------------------

print(f"Analyzing schema for: {csv_file_path}")
print("This may take a moment for large files...\n")

# Read only the first 1000 rows to quickly infer types.
# We use 'nrows' to make this fast, even for large files.
df = pd.read_csv(csv_file_path, nrows=1000)

print(f"Total Columns Found: {len(df.columns)}")
print(f"Total Rows Sampled: {len(df)}")
print("=" * 60)

# Initialize lists to store our report
column_names = []
dtypes = []
unique_values_sample = [] # Useful for seeing what's in a column
null_percentage = []

print(f"{'Column Name':<30} | {'Data Type':<10} | {'Null %':<8} | {'Sample Unique Values'}")
print("-" * 85)

for col in df.columns:
    col_dtype = df[col].dtype
    # Calculate null percentage
    null_pct = (df[col].isnull().sum() / len(df)) * 100
    # Get a sample of unique values for categorical columns
    if df[col].nunique() < 10: # If less than 10 unique values, show them all
        sample_vals = df[col].unique()
    else:
        sample_vals = df[col].iloc[:3].tolist() # Else, just show first 3 values
        if pd.api.types.is_numeric_dtype(col_dtype):
            sample_vals = [round(x, 2) for x in sample_vals if pd.notnull(x)]
    
    # Format the sample values for display to not clutter the output
    sample_str = str(sample_vals)
    if len(sample_str) > 40:
        sample_str = sample_str[:37] + "..."
    
    print(f"{col:<30} | {str(col_dtype):<10} | {null_pct:>6.1f}% | {sample_str}")
    
    # Store for the final summary
    column_names.append(col)
    dtypes.append(col_dtype)
    unique_values_sample.append(sample_vals)
    null_percentage.append(null_pct)

print("=" * 60)
print("\n--- RECOMMENDED SNOWFLAKE DATA TYPES ---")
print("-- Use this to build your CREATE TABLE statement --\n")

# Map pandas dtypes to Snowflake dtypes
type_map = {
    'int64': 'NUMBER(38, 0)',
    'float64': 'NUMBER(38, 2)', # Using 2 decimals for floats, common for financial data
    'object': 'VARCHAR(255)',
    'bool': 'BOOLEAN'
}

for col, pd_dtype in zip(column_names, dtypes):
    sf_dtype = type_map.get(str(pd_dtype), 'VARCHAR(255)') # Default to VARCHAR if unknown
    print(f"{col} {sf_dtype},")

print("\n--- SCRIPT END ---")
print("\nNext step: Copy the recommended 'Column Name SnowflakeDataType,' lines into your CREATE TABLE statement.")
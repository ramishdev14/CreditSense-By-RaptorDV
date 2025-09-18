import os
import json
import snowflake.connector
import datetime  # <-- Added this import
from snowflake.connector import DictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv('variables.env')

def get_snowflake_connection():
    """Establishes and returns a Snowflake connection."""
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

def profile_table(conn, table_name):
    """Profiles a given table and returns a JSON summary."""
    
    # Get basic table info and row count
    with conn.cursor(DictCursor) as cur:
        cur.execute(f"SELECT COUNT(*) AS row_count FROM {table_name}")
        global_stats = cur.fetchone()
        row_count = global_stats['ROW_COUNT']

    # Get column metadata
    with conn.cursor(DictCursor) as cur:
        cur.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        columns_metadata = cur.fetchall()

    column_stats = {}
    
    # Analyze each column
    for col in columns_metadata:
        col_name = col['COLUMN_NAME']
        data_type = col['DATA_TYPE']
        print(f"Profiling column: {col_name}")

        stats = {'dtype': data_type}
        
        with conn.cursor(DictCursor) as cur:
            # Calculate completeness (not null percentage)
            try:
                cur.execute(f"""
                    SELECT 
                        COUNT(*) AS total,
                        SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) AS null_count
                    FROM {table_name}
                """)
                completeness_result = cur.fetchone()
                null_count = completeness_result['NULL_COUNT']
                stats['completeness'] = 1.0 - (null_count / row_count)
                stats['missing_count'] = null_count
            except Exception as e:
                print(f"Error calculating completeness for {col_name}: {e}")
                stats['completeness'] = None
                stats['missing_count'] = None

            # Column-specific statistics based on data type
            if data_type in ('NUMBER', 'INTEGER', 'FLOAT', 'DECIMAL'):
                try:
                    cur.execute(f"""
                        SELECT 
                            MIN({col_name}) AS min_val,
                            MAX({col_name}) AS max_val,
                            AVG({col_name}) AS mean_val,
                            STDDEV({col_name}) AS std_dev,
                            SUM(CASE WHEN {col_name} = 0 THEN 1 ELSE 0 END) AS zeros
                        FROM {table_name}
                    """)
                    num_stats = cur.fetchone()
                    stats.update({
                        'min': float(num_stats['MIN_VAL']) if num_stats['MIN_VAL'] is not None else None,
                        'max': float(num_stats['MAX_VAL']) if num_stats['MAX_VAL'] is not None else None,
                        'mean': float(num_stats['MEAN_VAL']) if num_stats['MEAN_VAL'] is not None else None,
                        'std_dev': float(num_stats['STD_DEV']) if num_stats['STD_DEV'] is not None else None,
                        'zeros': num_stats['ZEROS']
                    })
                except Exception as e:
                    print(f"Error calculating numeric stats for {col_name}: {e}")

            elif data_type == 'VARCHAR':
                try:
                    cur.execute(f"""
                        SELECT 
                            COUNT(DISTINCT {col_name}) AS unique_count,
                            MODE({col_name}) AS most_common
                        FROM {table_name}
                    """)
                    char_stats = cur.fetchone()
                    stats.update({
                        'unique_values': char_stats['UNIQUE_COUNT'],
                        'most_common': char_stats['MOST_COMMON']
                    })
                except Exception as e:
                    print(f"Error calculating varchar stats for {col_name}: {e}")

        column_stats[col_name] = stats

    # Check for duplicate rows based on primary key (assuming SK_ID_CURR should be unique)
    with conn.cursor(DictCursor) as cur:
        try:
            cur.execute(f"""
                SELECT COUNT(*) AS duplicate_rows 
                FROM (
                    SELECT SK_ID_CURR, COUNT(*) 
                    FROM {table_name} 
                    GROUP BY SK_ID_CURR 
                    HAVING COUNT(*) > 1
                )
            """)
            duplicate_result = cur.fetchone()
            global_stats['duplicate_rows'] = duplicate_result['DUPLICATE_ROWS']
        except:
            global_stats['duplicate_rows'] = 0

    # Build the final JSON profile
    profile_json = {
        "table_name": table_name,
        "profile_timestamp": str(datetime.datetime.now()),  # <-- Fixed this line
        "column_stats": column_stats,
        "global_stats": {
            "row_count": row_count,
            "duplicate_rows": global_stats['duplicate_rows']
        }
    }
    
    return profile_json

def main():
    """Main function to run the profiling."""
    print("Starting Data Profiling...")
    
    conn = get_snowflake_connection()
    print("Connected to Snowflake.")
    
    try:
        # Profile our target table
        profile_data = profile_table(conn, "RAW_APPLICATION_DATA")
        
        # Save the profile to a JSON file (for the AI service to use)
        output_file = "data_profile.json"
        with open(output_file, 'w') as f:
            json.dump(profile_data, f, indent=2)
        
        print(f"Profiling complete! Results saved to {output_file}")
        print(f"Profiled {len(profile_data['column_stats'])} columns.")
        
    finally:
        conn.close()
        print("Snowflake connection closed.")

if __name__ == "__main__":
    main()
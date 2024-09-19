import snowflake.snowpark as snowpark

# Define your Snowflake credentials directly in the script
SNOWFLAKE_ACCOUNT = 'jlguadw-lw99714'
SNOWFLAKE_USER = 'MUDIYA'
SNOWFLAKE_PASSWORD = '94uU'
SNOWFLAKE_WAREHOUSE = 'MY_WAREHOUSES'
SNOWFLAKE_DATABASE = 'DEMO3DB'
SNOWFLAKE_SCHEMA = 'DEMO_TS'

def main(session: snowpark.Session):
    # Find the latest batch number
    latest_batch_query = """
    SELECT MAX(batch_number) AS latest_batch
    FROM DEMO3DB.DEMO_TS.LATEST_PROCESSED_CALLS
    """
    latest_batch_df = session.sql(latest_batch_query)
    latest_batch_number = latest_batch_df.collect()[0]['LATEST_BATCH']

    # Select data from the latest batch
    latest_batch_data_query = f"""
    SELECT *
    FROM DEMO3DB.DEMO_TS.LATEST_PROCESSED_CALLS
    WHERE batch_number = {latest_batch_number}
    """
    df_snowpark = session.sql(latest_batch_data_query)
    
    # Collect data as a list of rows
    data_rows = df_snowpark.collect()
    
    # Define the table name where the results will be stored
    result_table_name = "LATEST_BATCH_RESULTS"

    # Create or replace the table with the same schema as the source
    create_table_query = f"""
    CREATE OR REPLACE TABLE {result_table_name} AS
    SELECT * FROM DEMO3DB.DEMO_TS.LATEST_PROCESSED_CALLS
    WHERE batch_number = {latest_batch_number}
    """
    session.sql(create_table_query).collect()

if __name__ == "__main__":
    # Create a Snowflake session with hardcoded credentials
    session = snowpark.Session.builder.configs({
        "account": SNOWFLAKE_ACCOUNT,
        "user": SNOWFLAKE_USER,
        "password": SNOWFLAKE_PASSWORD,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA
    }).create()

    main(session)
    session.close()

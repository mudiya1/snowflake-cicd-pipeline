import snowflake.connector
import snowflake.snowpark as snowpark

# Connect to Snowflake with hardcoded credentials
conn = snowflake.connector.connect(
    user='MUDIYA',
    password='945uU',
    account='jlguadw-lw99714',
    warehouse='MY_WAREHOUSES',
    database='DEMO3DB',
    schema='DEMO_TS'
)

# Create a Snowpark session
session = snowpark.Session.builder.configs({
    'user': 'MUDIYA',
    'password': '945uU',
    'account': 'jlguadw-lw99714',
    'warehouse': 'MY_WAREHOUSES',
    'database': 'DEMO3DB',
    'schema': 'DEMO_TS'
}).create()

def execute_script():
    script_query = 'SELECT sql_command FROM your_script_table WHERE script_name = \'auto_execution.py\''

    # Fetch the Python script from Snowflake
    script_df = session.sql(script_query).to_pandas()
    script = script_df.iloc[0]['sql_command']
    
    # Execute the fetched Python script
    exec(script)

# Run the function to execute the script
execute_script()

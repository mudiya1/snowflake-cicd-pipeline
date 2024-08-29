import snowflake.connector

conn = snowflake.connector.connect(
    user='MUDIYA',         
    password='9490798615uU',
    account='jlguadw-lw99714.snowflakecomputing.com',
    warehouse='MY_WAREHOUSES',
    database='DEMO3DB',
    schema='DEMO_TS'
)

# Create a cursor object
cur = conn.cursor()

try:
    # Fetch the SQL command from Snowflake table
    cur.execute("SELECT sql_command FROM sql_scripts WHERE script_name = 'sample.sql'")
    sql_script = cur.fetchone()[0]  # Fetch the SQL command
    
    # Execute the fetched SQL command
    cur.execute(sql_script)
    print(f'Executed SQL script successfully.')
    
except Exception as e:
    print(f"Error executing SQL script: {e}")

# Close the connection
cur.close()
conn.close()


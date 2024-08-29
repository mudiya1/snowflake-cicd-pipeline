import snowflake.connector

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='MUDIYA',         
    password='9490798615uU',
    account='jlguadw-lw99714',
    warehouse='MY_WAREHOUSES',
    database='DEMO3DB',
    schema='DEMO_TS'
)

# Create a cursor object
cur = conn.cursor()

try:
    # Fetch the SQL command from Snowflake table
    cur.execute("SELECT sql_command FROM sql_scripts WHERE script_name = 'sample.sql'")
    sql_script = cur.fetchone()
    
    if sql_script:
        sql_script = sql_script[0]  # Fetch the SQL command
        print(f'Executing SQL script: {sql_script}')  # Print the SQL command
        
        # Execute the fetched SQL command
        cur.execute(sql_script)
        print('Executed SQL script successfully.')
    else:
        print("No SQL script found for 'sample.sql'.")
    
except Exception as e:
    print(f"Error executing SQL script: {e}")

# Close the connection
cur.close()
conn.close()

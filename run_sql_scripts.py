import snowflake.connector
import os

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
    # Read the SQL file
    with open('sample.sql', 'r') as file:
        sql_script = file.read()
    
    print(f'Executing SQL script: {sql_script}')  # Print the SQL command
    
    # Execute the SQL script
    cur.execute(sql_script)
    print('Executed SQL script successfully.')
    
except Exception as e:
    print(f"Error executing SQL script: {e}")

# Close the connection
cur.close()
conn.close()


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
    # Execute the worksheet by its name
    worksheet_name = 'sample.sql'  # The name of your worksheet
    
    # Execute the worksheet (this assumes it's a saved worksheet or named query)
    cur.execute(f"CALL SYSTEM$EXECUTE_WORKSHEET('{worksheet_name}');")
    
    print('Executed worksheet successfully.')
    
except Exception as e:
    print(f"Error executing worksheet: {e}")

# Close the connection
cur.close()
conn.close()

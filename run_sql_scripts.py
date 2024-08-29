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
    cur.execute("SELECT sql_command FROM sql_commands WHERE script_name = 'sample.sql'")
    sql_script = cur.fetchone()[0]  # Fetch the SQL command

    # Split the SQL script into individual statements
    sql_statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]

    # Execute each SQL statement individually
    for statement in sql_statements:
        cur.execute(statement)
        print(f'Executed SQL statement: {statement}')

    print('All SQL statements executed successfully.')

except Exception as e:
    print(f"Error executing SQL script: {e}")

# Close the connection
cur.close()
conn.close()

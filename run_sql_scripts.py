import snowflake.connector
import os

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv('USER'),
    password=os.getenv('PASSWORD'),
    account=os.getenv('ACCOUNT'),
    warehouse=os.getenv('WAREHOUSE'),
    database=os.getenv('DATABASE'),
    schema=os.getenv('SCHEMA')
)

# Create a cursor object
cur = conn.cursor()

# List of SQL files to execute
sql_files = ['file1.sql', 'file2.sql']

for sql_file in sql_files:
    with open(sql_file, 'r') as file:
        sql_script = file.read()
        cur.execute(sql_script)
        print(f'Executed {sql_file}')

# Close the connection
cur.close()
conn.close()


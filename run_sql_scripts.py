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

# List of actual SQL files to execute
sql_files = ['path/to/your/sql/file1.sql', 'path/to/your/sql/file2.sql']  # Replace with your actual SQL file paths

for sql_file in sql_files:
    try:
        with open(sql_file, 'r') as file:
            sql_script = file.read()
            cur.execute(sql_script)
            print(f'Executed {sql_file} successfully.')
    except FileNotFoundError:
        print(f"Error: The file {sql_file} was not found.")
    except Exception as e:
        print(f"Error executing {sql_file}: {e}")

# Close the connection
cur.close()
conn.close()

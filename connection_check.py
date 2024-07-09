import snowflake.snowpark.functions
from snowflake.snowpark import Session

# CHECK YOUR DETAILS IN SNOWFLAKE & CHANGE ACCORDINGLY
connection_parameters = {"account":"gjb88616.us-east-1",
"user":"teja",
"password": "Teja@9999",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"SAM",
"schema":"PUBLIC"
}
try:
    # Create a Snowflake session
    session = Session.builder.configs(connection_parameters).create()
    print("Connection successful")

    # Run a simple query
    result = session.sql("SELECT current_warehouse(), current_database(), current_schema()").collect()
    print(result)
    
except Exception as e:
    print("Error connecting to Snowflake:", e)

import snowflake.snowpark.functions
from snowflake.snowpark import Session

# CHECK YOUR DETAILS IN SNOWFLAKE & CHANGE ACCORDINGLY
connection_parameters = {"account":"ENTER_YOUR_ACCOUNT_IDENTIFIER_HERE",
"user":"ENTER_YOUR_USER_NAME_HERE",
"password": "ENTER_YOUR_PASSWORD_HERE",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",#DEFAULT WAREHOUSE
"database":"DATABASE_NAME_HERE",
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

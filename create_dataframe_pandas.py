from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import pandas as pd
import time
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType


# Replace the below connection_parameters with your respective snowflake account,user name and password
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
    
    # Option 1: Create a pandas DataFrame
    pandas_df = pd.DataFrame({"a": [1], "b": [2], "c": [3], "d": [4], "e": [5]})
    
    # Option 2: Create a list of tuples
    data = [(1, 2, 3, 4, 5)]
    
    # Option 3: Create a list of lists
    data_list = [[1, 2, 3, 4, 5]]
    
    # Create a Snowflake DataFrame using a list of tuples
    snowpark_df = session.create_dataframe(data, ["a", "b", "c", "d", "e"])
    snowpark_df.show()
    
    # Show the type of the Snowflake DataFrame
    print(type(snowpark_df))
    
    # Load an existing table
    test2 = session.table("sam.PUBLIC.A")
    test2.show()

except Exception as e:
    print("Error during Snowflake operations:", e)
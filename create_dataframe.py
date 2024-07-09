from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import time

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

# Replace the below connection_parameters with your respective snowflake account,user name and password
connection_parameters = {"account":"ACCOUNT_NAME",
"user":"USERNAME",
"password": "PASSWORD",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DATABASE_NAME",
"schema":"PUBLIC"
}
try:
    # Create a Snowflake session
    session = Session.builder.configs(connection_parameters).create()
    print("Connection successful")
    
    # Use a specific warehouse
    session.sql("USE WAREHOUSE COMPUTE_WH").collect()

    # Example 1: Creating a DataFrame with an explicit schema
    test1 = session.create_dataframe([[1, 2, 3, 4]], schema=["a", "b", "c", "d"])
    test1.show()

    # Example 2: Creating a DataFrame with inferred schema
    test2 = session.create_dataframe([[1, 2, 3, "123"], [1, 2, 3, "ABC"], [1, 2, 3, "HPC"], [1, 2, 3, "EMD"]])
    test2.show()

    # Example 3: Handling date types
    test3 = session.create_dataframe([[1, 2, 3, '2022-01-26'], [1, 2, 3, '2022-01-27']], schema=["a", "b", "c", "d"])
    test3.show()

    # Example 4: Handling float types
    test4 = session.create_dataframe([[1, 2, 3, 26.897], [1, 2, 3, 27.897]], schema=["a", "b", "c", "d"])
    test4.show()

    # Example 5: Handling None values
    test5 = session.create_dataframe([[1, 2, 3, None], [1, 2, 3, None]], schema=["a", "b", "c", "d"])
    test5.show()

    # Example 6: Handling complex types (dict)
    test6 = session.create_dataframe([[1, 2, 3, {"a": "hi"}], [1, 2, 3, None], [1, 2, 3, {"a": "Bye"}]])
    test6.show()

    # Example 7: Handling complex types (list)
    test7 = session.create_dataframe([[1, 2, 3, ["Hi"]], [1, 2, 3, None]])
    test7.show()

    # Cache the result of test7
    cached_test7 = test7.cache_result()
    cached_test7.show()

    # Check performance difference between test7 and cached_test7
    begin = time.time()
    test7.show()
    end = time.time()
    print(f"Total runtime of test7 is {end - begin}")

    begin = time.time()
    cached_test7.show()
    end = time.time()
    print(f"Total runtime of cached_test7 is {end - begin}")

except Exception as e:
    print("Error during Snowflake operations:", e)
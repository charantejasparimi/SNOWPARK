import os
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
connection_parameters = {"account":"ENTER_YOUR_ACCOUNT_IDENTIFIER_HERE",
"user":"ENTER_YOUR_USER_NAME_HERE",
"password": "ENTER_YOUR_PASSWORD_HERE",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",#DEFAULT WAREHOUSE
"database":"DATABASE_NAME_HERE",
"schema":"PUBLIC"
}
test_session = Session.builder.configs(connection_parameters).create()
print(test_session.sql("select current_warehouse(), current_database(), current_schema()").collect())
session = Session.builder.configs(connection_parameters).create()
session.sql("USE WAREHOUSE COMPUTE_WH").collect()
df_customer_info = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER")
df_customer_filter = df_customer_info.filter(col("C_MKTSEGMENT") == 'HOUSEHOLD')
df_customer_select = df_customer_info.select(col("C_NAME"), col("C_ADDRESS"))
df_customer_select.show(2)
df_customer_select.count()
df_customer_select.describe().sort("SUMMARY").show()
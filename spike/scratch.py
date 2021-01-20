from st_connectors.db.snowflake.client import SnowflakeConnector

conn_dict = {
    "host": "https://tq10634.us-east-1.snowflakecomputing.com",
    "username": "IRP_DEV",
    "password": "pASSWORD123",
    "database": "SSR",
    "role": "IRP_ACS_DEV",
    "schema": "dev",
    "warehouse": "WH_IRP_ETL",
}
wh_db_connection = SnowflakeConnector(conn_dict)
results = wh_db_connection.execute_query("select * from account")
print(results)

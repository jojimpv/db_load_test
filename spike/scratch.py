from st_connectors.db.oracle.client import OracleConnector

conn_dict = {
    "host": "localhost",
    "username": "ot",
    "password": "Accion2020",
    "dbname": "ot",
    "port":"1527",
    "sid":"OraDoc"

}
wh_db_connection = OracleConnector(conn_dict)
results = wh_db_connection.execute_query("""
select * from persons
""")
print(results)

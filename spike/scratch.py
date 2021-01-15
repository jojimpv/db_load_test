from st_connectors.db.oracle.client import OracleConnector

conn_dict = {"host":"localhost", "username":"ot", "password":"Accion2020","sid":"OraDoc","port":"1527"}
wh_db_connection = OracleConnector(conn_dict)
results = wh_db_connection.execute_query("select * from customers where customer_id = '73'")
print(results)

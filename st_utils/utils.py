from st_connectors.db.hive.client import HiveConnector
from st_connectors.db.mySql.client import MySQLConnector
from st_connectors.db.oracle.client import OracleConnector
from st_connectors.db.postgresql.client import PostgresConnector
from st_connectors.db.snowflake.client import SnowflakeConnector


def get_connector(db_type):
    if db_type == "Oracle":
        return OracleConnector
    if db_type == "Snowflake":
        return SnowflakeConnector
    if db_type == "Postgres":
        return PostgresConnector
    if db_type == "MySql":
        return MySQLConnector
    if db_type == "Hive":
        return HiveConnector

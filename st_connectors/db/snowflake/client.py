import sys

from st_utils.logger import get_logger

logger = get_logger(__name__)


class SnowflakeConnector:
    def __init__(self, conn_dict):

        logger.info("Initiating snowflake database connection")
        try:
            self.sf_url = conn_dict["host"]
            self.sf_user = conn_dict["username"]
            self.sf_password = conn_dict["password"]
            self.sf_database = conn_dict["database"]
            self.sf_role = conn_dict["role"]
            self.sf_schema = conn_dict["schema"]
            self.sf_warehouse = conn_dict["warehouse"]

            self.options = dict(
                sfUrl=self.sf_url,
                sfUser=self.sf_user,
                sfPassword=self.sf_password,
                sfDatabase=self.sf_database,
                sfSchema=self.sf_schema,
                sfWarehouse=self.sf_warehouse,
                sfRole=self.sf_role,
            )
        except Exception as error:
            logger.error(f"Snowflake connection failed - {error}")

    def execute_query(self, query):
        from snowflake.connector import connect

        try:
            conn = connect(
                user=self.sf_user,
                password=self.sf_password,
                account=(self.sf_url)
                .replace("https://", "")
                .replace(".snowflakecomputing.com", ""),
                warehouse=self.sf_warehouse,
                database=self.sf_database,
                schema=self.sf_schema,
            )
            cur = conn.cursor()
            cur.execute(query)
            return cur.sfqid
        except Exception as error:
            logger.error(f"error in execute_query  {error}")
            raise

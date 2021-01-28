"""
MySQL Connector Class
"""
import mysql.connector as mysql

from st_utils.logger import get_logger

logger = get_logger(__name__)


class MySQLConnector:
    """
    MySQL Connector Class
    """

    def __init__(self, conn_dict):
        logger.debug("Initiating MySQL database connection")
        self.connection = None
        try:
            self.host = conn_dict["host"]
            self.username = conn_dict["username"]
            self.password = conn_dict["password"]
            self.dbname = conn_dict["dbname"]
            self.port = conn_dict["port"]

            self._db_connection = mysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.dbname,
                connection_timeout=10,
            )

            self._db_cur = self._db_connection.cursor()

            logger.debug(f"Connection established to MySQL database using {self.host}")
        except mysql.Error as err:
            logger.error(
                "Database connection failed due to MySQL state {}".format(
                    " ".join(err.args)
                )
            )

    def __enter__(self):
        logger.debug("Return self")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logger.debug("Closing MySQL database connection")
        self._db_cur.close()
        self._db_connection.close()

    def get_column_info(self, query):
        """

        Args:
            query (str): Query String

        Returns:
            list: Column Names from query

        """
        self._db_cur.execute(query)
        result = self._db_cur
        result.fetchall()
        return result.column_names

    def execute_query(self, query):
        """

        Args:
            query (str): Query String
            raise_error (bool): True means it will raise error else not

        Returns:
            cursor: Database Cursor

        """
        print(f"Executing {query} on MySQL")
        try:
            self._db_cur.execute(query)
            try:
                result = self._db_cur.fetchall()
                if result is not None:
                    for row in result:
                        return row[0]
            except Exception:
                pass
            return None
        except Exception as error:
            logger.error(f'error executing query "{query}", error: {error}')
            raise

    def execute_commit(self):
        """
        Commit the transaction

        Returns:
            bool:

        """
        try:
            self._db_connection.commit()
        except Exception as error:
            logger.error(f"error in db commit {error}")
            return True
        else:
            return False

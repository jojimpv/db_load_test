"""

"""
import pymssql

from st_utils.logger import get_logger

logger = get_logger(__name__)


class SqlServerConnector:

    def __init__(self, conn_dict):
        logger.info("Initiating postgres database connection")
        self.connection = None
        try:
            server = conn_dict["host"]
            user = conn_dict["username"]
            password = conn_dict["password"]
            database = conn_dict["dbname"]
            port = conn_dict["port"]
            self._db_connection = pymssql.connect(server=server, user=user, password=password,port=port,  database=database)
            self._db_cur = self._db_connection.cursor()
            logger.info(f"Connection established to SQLServer database using {server}")
        except pymssql.Error as err:
            logger.error(
                "Database connection failed due to SQLServer state {}".format(
                    " ".join(err.args)
                )
            )

    def __exit__(self):
        logger.info("Closing SQLServer database connection")
        self._db_connection.close()
        return self._db_cur.close()

    def execute_query(self, query):
        logger.debug(f"Executing {query} on SQLServer")
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
        finally:
            self._db_connection.commit()

    def execute_commit(self):
        try:
            self._db_connection.commit()
        except Exception as error:
            logger.error(f"error in db commit {error}")
            return True
        else:
            return False

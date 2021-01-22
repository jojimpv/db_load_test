"""

"""
import psycopg2
import psycopg2.extras

from st_utils.logger import get_logger

logger = get_logger(__name__)


class PostgresConnector:
    def __init__(self, conn_dict):
        logger.info("Initiating postgres database connection")
        self.connection = None
        try:
            host = conn_dict["host"]
            username = conn_dict["username"]
            password = conn_dict["password"]
            dbname = conn_dict["dbname"]
            port = conn_dict["port"]
            using = (
                f"dbname='{dbname}' user='{username}' host='{host}' "
                f"password='{password}' port='{port}'"
            )
            self._db_connection = psycopg2.connect(using)
            self._db_cur = self._db_connection.cursor(
                cursor_factory=psycopg2.extras.DictCursor
            )
            self.url = f"jdbc:postgresql://{host}:{port}/{dbname}"

            self.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            self.properties = {
                "user": username,
                "password": password,
                "driver": "org.postgresql.Driver",
            }
            logger.info(f"Connection established to postgres database using {using}")
        except psycopg2.Error as err:
            logger.error(
                "Database connection failed due to postgres state {}".format(
                    " ".join(err.args)
                )
            )

    def __exit__(self):
        logger.info("Closing postgres database connection")
        self._db_connection.close()
        return self._db_cur.close()

    def execute_query(self, query):
        logger.debug(f"Executing {query} on postgres")
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

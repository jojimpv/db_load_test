"""

"""
import logging

import psycopg2
import psycopg2.extras


class PostgresConnector:
    def __init__(self, conn_dict):
        logging.info("Initiating postgres database connection")
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
            logging.info(f"Connection established to postgres database using {using}")
        except psycopg2.Error as err:
            logging.error(
                "Database connection failed due to postgres state {}".format(
                    " ".join(err.args)
                )
            )

    def __exit__(self):
        logging.info("Closing postgres database connection")
        self._db_connection.close()
        return self._db_cur.close()

    def execute_query(self, query):
        logging.debug(f"Executing {query} on postgres")
        try:
            self._db_cur.execute(query)
            result = self._db_cur
            if result is not None:
                for row in result:
                    return row[0]
            return None
        except Exception as error:
            logging.error(f'error executing query "{query}", error: {error}')
            return None
        finally:
            self._db_connection.commit()

    def execute_commit(self):
        try:
            self._db_connection.commit()
        except Exception as error:
            logging.error(f"error in db commit {error}")
            return True
        else:
            return False

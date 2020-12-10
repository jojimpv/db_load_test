"""

"""
import logging

import cx_Oracle


class OracleConnector:
    def __init__(self, conn_dict):
        logging.info("Initiating Oracle database connection")
        cx_Oracle.init_oracle_client("/Users/dc/instantclient_19_8")

        try:
            host = conn_dict["host"]
            username = conn_dict["username"]
            password = conn_dict["password"]
            SID = conn_dict["sid"]
            port = conn_dict["port"]
            dsn_tns = cx_Oracle.makedsn(host, port, SID)
            self._db_connection = cx_Oracle.connect(username, password, dsn_tns)
            self._db_cur = self._db_connection.cursor()
            logging.info(f"Connection established to Oracle database at {host}:{port}")
        except cx_Oracle.Error as err:
            logging.error(
                "Database connection failed due to postgres state {}".format(
                    " ".join(err.args)
                )
            )

    def __exit__(self):
        logging.info("Closing Oracle database connection")
        self._db_connection.close()
        return self._db_cur.close()

    def execute_query(self, query):
        logging.debug(f"Executing {query} on Oracle")
        try:
            self._db_cur.execute(query)
            result = self._db_cur
            if result is not None:
                self._db_connection.commit()
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

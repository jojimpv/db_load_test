import csv
from datetime import datetime

from st_utils.logger import get_logger
from st_utils.utils import get_connector

logger = get_logger(__name__)


def put_raw_data(db_type, db_connect, run_name, result_list, table_name, root_path):
    logger.info("executing put_raw_data")
    convert_str_to_list = [
        [
            run_name,
            i.rstrip("#").split("#")[0],
            i.rstrip("#").split("#")[1],
            i.rstrip("#").split("#")[2],
        ]
        for i in result_list
    ]
    if table_name != "":
        connector_type = get_connector(db_type)
        connector = connector_type(db_connect)
        # Oracle does not have IF NOT EXISTS
        if db_type == "Oracle":
            t_query = f"select count(*) from dba_tables where upper(table_name)=upper('{table_name}') "
            result = connector.execute_query(t_query)
            if int(result) == 0:
                connector.run_ddl(
                    f"""create table {table_name} (RUN_NAME varchar(200),
                                                                    QUERY_NAME varchar(100),
                                                                    query_id varchar(200), python_time varchar(200)
                                                                )"""
                )
        else:
            connector.execute_query(
                f"""create table {table_name} IF NOT EXISTS (RUN_NAME varchar(200),
                                                    QUERY_NAME varchar(100),
                                                    query_id varchar(200), python_time varchar(200)
                                                )"""
            )

        for row in convert_str_to_list:
            query = f""" insert into {table_name} values
                            ( '{row[0]}', '{row[1]}', '{row[2]}', '{row[3]}')
                    """

            connector.execute_query(query)
            try:
                connector.execute_commit()
            except Exception:
                pass

    dt = datetime.now()
    int_date = int(dt.strftime("%Y%m%d%H%M%S"))
    logger.info("Creating csv")
    with open(f"download_reports/{run_name}_{int_date}.csv", "w", newline="") as f:
        writer = csv.writer(f)
        if db_type == "Snowflake":
            writer.writerow(["RUN NAME", "REPORT NAME", "QUERY_ID", "TIME"])
        else:
            writer.writerow(
                ["RUN NAME", "REPORT NAME", "RESULT(FIRST ROW_COL)", "TIME"]
            )
        writer.writerows(convert_str_to_list)
        logger.info("done creating report")
    return f"download_reports/{run_name}_{int_date}.csv"

# from st_connectors.db.oracle.client import OracleConnector
#
# conn_dict = {
#     "host": "localhost",
#     "username": "ot",
#     "password": "Accion2020",
#     "dbname": "ot",
#     "port":"1527",
#     "sid":"OraDoc"
#
# }
# wh_db_connection = OracleConnector(conn_dict)
# results = wh_db_connection.execute_query("""
# select * from persons
# """)
# print(results)
import pandas as pd
from pyspark.sql import SparkSession

from excel.client import ExcelConnector
from jobs.query_builder import build_query


def create_query(file_name, query_id_list, total_limit):
    total_queries_list = []
    query_df = pd.read_excel(file_name, sheet_name="Sheet1")
    param_cols = [i for i in query_df.columns if "param" in str(i).lower()]
    qid_str = [str(i) for i in query_id_list]
    query_df = query_df.loc[query_df["qid"].isin(qid_str)]
    count_of_query = len(query_id_list)
    limit_each = int(total_limit / count_of_query)
    query_list = []

    for i, row in enumerate(query_df.itertuples(), 1):
        query_list.append(row)
    for idx, item in enumerate(query_list):
        print(item)
        for i in range(limit_each):
            tup_ret = build_query(item, param_cols, limit_each)
            total_queries_list.append(tup_ret)
    return total_queries_list


def create_query_spark(spark, file_name, query_id_list, total_limit):
    total_queries_list = []
    excel = ExcelConnector(file_name, "Sheet1")
    query_df = excel.read_excel(spark)
    param_cols = [i for i in query_df.columns if "param" in str(i).lower()]
    qid_str = [str(i) for i in query_id_list]
    filter_str = ",".join(qid_str)
    print("filter string", filter_str)
    query_df = query_df.filter(f"qid in ({filter_str}) ")

    count_of_query = len(query_id_list)
    limit_each = int(total_limit / count_of_query)

    query_list = query_df.collect()

    for idx, item in enumerate(query_list):
        print(item)

        for i in range(limit_each):
            tup_ret = build_query(item, param_cols, limit_each)
            total_queries_list.append(tup_ret)
    return total_queries_list


# spark = (
#     SparkSession.builder.appName("simple-sf-test")
#     .config(
#         "spark.jars.packages",
#         "net.snowflake:spark-snowflake_2.11:2.7.0-spark_2.4,"
#         "org.postgresql:postgresql:42.2.9.jre7",
#     )
#     .getOrCreate()
# )
query_id_list = [1, 2]
# a= create_query_spark(spark, "/Users/dc/Desktop/oracle_query.xlsx",query_id_list, 2)
# print(a)

a = create_query("/Users/dc/Desktop/oracle_query.xlsx", query_id_list, 2)
print(a)

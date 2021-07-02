
import asyncio
import functools
import re
import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession

from jobs.query_builder import create_query
from jobs.report_builder import build_report
from st_utils.logger import get_logger
from project_settings import settings, get_root_path

logger = get_logger(__name__)
root_path = get_root_path()
from st_connectors.db.snowflake.client import SnowflakeConnector

snow = SnowflakeConnector(sf_url= settings.sfUrl,
                          sf_user=settings.SFUSER,
                          sf_password=settings.SFPASSWORD,
                          sf_role=settings.SFROLE,
                          sf_warehouse=settings.SFWAREHOUSE,
                          sf_schema=settings.SFSCHEMA,
                          sf_database=settings.SFDATABASE)

def force_async(fn):
    from concurrent.futures import ThreadPoolExecutor
    import asyncio
    pool = ThreadPoolExecutor()

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        future = pool.submit(fn, *args, **kwargs)
        return asyncio.wrap_future(future)  # make it awaitable

    return wrapper


@force_async
def execute_query(q_name, query):
    logger.info('executing query')
    cur = snow.get_cursor()
    start_time = time.monotonic()
    cur.execute(query)
    sf_qid = (cur.sfqid)
    result = cur.fetchall()
    end_time = time.monotonic()
    ret_result = f'{q_name}#{sf_qid}#{end_time - start_time}#'
    return ret_result

def query_executor(spark, file_name, query_id_list, total_limit, stagger_for):
    query_list = create_query(spark, file_name, query_id_list, total_limit)
    start_time = time.monotonic()
    logger.info(f'time now: {datetime.utcnow()}')

    tasks = []
    final_data = []
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        for query in query_list:
            logger.info(f"{query[1]}")
            pg_result = execute_query(query[0], query[1])
            time.sleep(int(stagger_for))
            tasks.append(pg_result)
        logger.info(f'async tasks: {tasks}')
        futures, _ = loop.run_until_complete(asyncio.wait(tasks))
        for future in futures:
            result_part = future.result()
            final_data.append(result_part)

        end_time = time.monotonic()
        logger.info(f'time_diff: {end_time - start_time}')
        return final_data
    finally:
        loop.close()


def main(spark, file_name, run_name , query_id_list=None, total_limit=10, run_for=60,
                                                                        stagger_for=0):
    query_res_list = []
    start_time = time.monotonic()
    while True:
        query_list = query_executor(spark, file_name, query_id_list,
                                    total_limit, stagger_for)
        query_res_list.extend(query_list)
        end_time = time.monotonic()
        time.sleep(2)
        if end_time - start_time > run_for:
            break
    build_report(spark, run_name, query_res_list)

if __name__ == '__main__':

    run_name = sys.argv[1]
    query_id_list = eval(sys.argv[2])
    total_limit = int(sys.argv[3])
    run_for = int(sys.argv[4])
    stagger_for = int(sys.argv[5])

    if len(query_id_list) > int(total_limit):
        print('''
        total limit should be more than number of queries.
        if qids is 1,2,3 the minimum total number of runs has to be 3 ''')
        exit(1)


    spark = SparkSession \
            .builder \
            .appName("iqvia-uat_cancer") \
            .config('spark.jars.packages',
                    'net.snowflake:spark-snowflake_2.11:2.7.0-spark_2.4,'
                    'org.postgresql:postgresql:42.2.9.jre7'
                    ) \
            .getOrCreate()

    main(spark,
         file_name=f"file://{root_path}/jobs/queries-with-names-and-parameters.xlsx",
         query_id_list = query_id_list,
         run_name=run_name,
         total_limit=total_limit,
         run_for=run_for,
         stagger_for=stagger_for)

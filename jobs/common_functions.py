import asyncio
import functools
import time
from datetime import datetime

from jobs.query_builder import create_query
from st_utils.logger import get_logger
from st_utils.utils import get_connector

logger = get_logger(__name__)


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
def execute_query(q_name, query, db_type, db_connect):
    connector_type = get_connector(db_type)
    connector = connector_type(db_connect)
    logger.info(f"executing query {query}")
    start_time = time.monotonic()
    result = connector.execute_query(query)
    end_time = time.monotonic()
    ret_result = f"{q_name}#{result}#{end_time - start_time}#"
    return ret_result


def query_executor(db_name, db_connect, file_name, query_id_list=None, total_limit=10):
    query_list = create_query(file_name, query_id_list, total_limit)
    start_time = time.monotonic()
    logger.info(f"time now: {datetime.utcnow()}")

    tasks = []
    final_data = []
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        for query in query_list:
            pg_result = execute_query(query[0], query[1], db_name, db_connect)
            tasks.append(pg_result)
        logger.info(f"async tasks: {tasks}")
        futures, _ = loop.run_until_complete(asyncio.wait(tasks))
        for future in futures:
            result_part = future.result()
            final_data.append(result_part)

        end_time = time.monotonic()
        logger.info(f"time_diff: {end_time - start_time}")
        return final_data
    finally:
        loop.close()

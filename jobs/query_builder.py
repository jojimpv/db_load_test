import random
import re

from pyspark.sql import SparkSession

from excel.client import ExcelConnector
from st_utils.logger import get_logger

logger = get_logger(__name__)


def find_between_r(given_str, first, last):
    start = given_str.find(first) + 8
    end = given_str.find(last, start)
    return (given_str[start:end])



def build_query(each_row, list_of_params, number_required):
    query_to_build = each_row.query
    qid = each_row.report_name
    for idx, item in enumerate(list_of_params):
        idx = idx + 1
        eval_param = eval(f"each_row.param{idx}")
        if eval_param is not None:
            param = eval_param.rstrip(',').split(",")
            for i in range(number_required):
                random_param = random.choice(param)
                param_rand = f"$PARAM{idx}:"
                if param_rand in query_to_build:
                    result = find_between_r(query_to_build,param_rand, "#")
                    number_of_params = random.randint(1, int(result))
                    to_replace = random.sample(param, number_of_params)
                    to_replace = [f"'{i}'" for i in to_replace]
                    to_rep_str = ",".join(to_replace)
                    query_to_build = query_to_build.replace(f"$PARAM{idx}:{result}#", to_rep_str)
                if f"$PARAM{idx}" in query_to_build:
                    query_to_build = query_to_build.replace(f"$PARAM{idx}", f"'{random_param}'")
    tup_ret = (qid, query_to_build)
    return tup_ret


def create_query(spark, file_name, query_id_list, total_limit=20):
    total_queries_list = []
    excel = ExcelConnector(file_name, "Sheet1")
    query_df = excel.read_excel(spark)
    param_cols = [i  for i in query_df.columns if "param" in str(i).lower()]
    qid_str = [str(i) for i in query_id_list]
    filter_str = ",".join(qid_str)
    query_df = query_df.filter(f'qid in ({filter_str}) ')

    count_of_query = len(query_id_list)
    limit_each = int(total_limit/count_of_query)

    query_list = query_df.collect()

    for idx, item in enumerate(query_list):
        for i in range(limit_each):
            tup_ret = build_query(item, param_cols, limit_each)
            total_queries_list.append(tup_ret)
    return total_queries_list


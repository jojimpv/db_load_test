import random
import re

from excel.client import ExcelConnector
from st_utils.logger import get_logger

logger = get_logger(__name__)


def param_occurances_multiple_values(input_str, param, param_list):
    resutls = re.findall(rf"{param}:(.*?)#", input_str)
    for replace_values in resutls:
        random_param = ""
        for i in range(int(replace_values)):
            random_param = f"{random_param},'{random.choice(param_list)}'"
        input_str = input_str.replace(
            f"${param}:{int(replace_values)}#", random_param.lstrip(",")
        )
    return input_str


def param_occurances_single_value(input_str, param, param_list):
    random_param = random.choice(param_list)
    input_str = input_str.replace(f"${param}", f"'{random_param}'")
    return input_str


def build_query(each_row, list_of_params, number_required):
    query_to_build = each_row.query
    qid = each_row.report_name
    for idx, item in enumerate(list_of_params):
        idx = idx + 1
        eval_param = eval(f"each_row.param{idx}")
        if eval_param is not None:
            param_list = eval_param.rstrip(",").split(",")
            for i in range(number_required):
                query_to_build = param_occurances_multiple_values(
                    query_to_build, f"PARAM{idx}", param_list
                )
                query_to_build = param_occurances_single_value(
                    query_to_build, f"PARAM{idx}", param_list
                )
    tup_ret = (qid, query_to_build)
    print("tup_ret", tup_ret)
    return tup_ret


def create_query(spark, file_name, query_id_list, total_limit):
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
        for i in range(limit_each):
            tup_ret = build_query(item, param_cols, limit_each)
            total_queries_list.append(tup_ret)
    return total_queries_list

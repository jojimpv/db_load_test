import random
import re

import pandas as pd

from st_utils.logger import get_logger

logger = get_logger(__name__)


def param_occurances_multiple_values(input_str, param, param_list):
    resutls = re.findall(rf"{param}:(.*?)#", input_str)
    for replace_values in resutls:
        random_param = ""
        random_sample = random.sample(param_list, k=random.randint(1, int(replace_values)))
        random_sample_str = ','.join([f"'{x}'" for x in random_sample])
        random_param = f"{random_param},{random_sample_str}"
        # input_str = input_str.replace(
        #     f"${param}:{int(replace_values)}#", random_param.lstrip(",")
        param_is = f"\${param}"
        input_str = re.sub(
            rf"\B{param_is}\b:{int(replace_values)}#", random_param.lstrip(","), input_str
        )

    return input_str


def param_occurances_single_value(input_str, param, param_list):
    random_param = random.choice(param_list)
    param_is = f"\${param}"
    input_str = re.sub(rf"\B{param_is}\b", f"'{random_param}'", input_str)
    return input_str


def build_query(each_row, list_of_params, number_required):
    query_to_build = each_row.query
    qid = each_row.report_name
    for idx, item in enumerate(list_of_params):
        idx = idx + 1
        eval_param = eval(f"each_row.param{idx}")
        if eval_param is not None:
            param_list = str(eval_param).rstrip(",").split(",")
            for i in range(number_required):
                query_to_build = param_occurances_multiple_values(
                    query_to_build, f"PARAM{idx}", param_list
                )
                query_to_build = param_occurances_single_value(
                    query_to_build, f"PARAM{idx}", param_list
                )
    tup_ret = (qid, query_to_build)
    return tup_ret


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
        for i in range(limit_each):
            tup_ret = build_query(item, param_cols, limit_each)
            total_queries_list.append(tup_ret)
    return total_queries_list

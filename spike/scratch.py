import random
import re

def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""

def param_occurances_multiple_values(input_str, param, param_list):
    resutls = re.findall(rf'{param}:(.*?)#', input_str)
    for replace_values in resutls:
        random_param = ""
        for i in range(int(replace_values)):
            random_param = f"{random_param},'{random.choice(param_list)}'"
        print(f"{param}:{int(replace_values)}#", "random shit")
        input_str = input_str.replace(f"${param}:{int(replace_values)}#", random_param.lstrip(","))
    return input_str

def param_occurances_single_value(input_str, param, param_list):
    random_param = random.choice(param_list)
    input_str = input_str.replace(f"${param}", f"'{random_param}'")
    return input_str


s = "select * from countries where region_id in( $PARAM1:10# ) and space_id in ($PARAM1:2#) and shit_id = $PARAM1"
param_list = ['we', '1', 'er']
new_str = param_occurances_multiple_values(s,"PARAM1", param_list)
new_str = param_occurances_single_value(new_str, "PARAM1", param_list)
print(new_str)
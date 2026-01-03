import pandas as pd
import re
import operator


def filter_dataframe(input_df, keep_attribute, keep_values):
    # Filter by a list of values to be kept

    everything = []

    # Iterate over all rows:
    # Retrieve using column index, not colname - this is faster:
    colidx = input_dataframe.columns.get_loc(keep_attribute)
    for row in input_df.itertuples(index=False):
        curr_value = row[colidx]

        if curr_value in keep_values:
            # keep!!
            # Collect results in list:
            everything.append(row)

    # Finished collecting the results, now make pandas dataframe:
    dataframe = pd.DataFrame(everything)
    return dataframe

def filter_dataframe_by_condition(input_df, keep_attribute, condition_dict):
    # Filter by numeric condition

    everything = []

    # Iterate over all rows:
    # Retrieve using column index, not colname - this is faster:
    colidx = input_dataframe.columns.get_loc(keep_attribute)
    for row in input_df.itertuples(index=False):
        curr_value = row[colidx]

        if matches_filter_condition(condition_dict, curr_value):
            # keep!!
            # Collect results in list:
            everything.append(row)

    # Finished collecting the results, now make pandas dataframe:
    dataframe = pd.DataFrame(everything)
    return dataframe


def parse_filter_condition(expr, var="x"):
    # Written by ChatGPT
    expr = expr.replace(" ", "")

    # Needed to flip them, in case the user puts value first
    # E.g.: "200>x" instead of "x<200": Operator is reversed!
    FLIP_OP = {
        "<": ">",
        "<=": ">=",
        ">": "<",
        ">=": "<=",
        "==": "=="
    }

    # Normalize single '=' to '==', but keep <=, >=, ==
    expr = re.sub(r'(?<![<>=])=(?![=])', '==', expr)

    # Range: 100<x<200
    m = re.fullmatch(rf"(-?\d+(?:\.\d+)?)<{var}<(-?\d+(?:\.\d+)?)", expr)
    if m:
        return {
            "type": "range",
            "min": float(m.group(1)),
            "max": float(m.group(2))
        }

    # Comparison with variable: x<200, x==200
    m = re.fullmatch(rf"{var}(<=|>=|<|>|==)(-?\d+(?:\.\d+)?)", expr)
    if m:
        return {
            "type": "comparison",
            "op": m.group(1),
            "value": float(m.group(2))
        }

    # Comparison, value on the left: 200>x, 200==x
    m = re.fullmatch(rf"(-?\d+(?:\.\d+)?)(<=|>=|<|>|==){var}", expr)
    if m:
        return {
            "type": "comparison",
            "op": FLIP_OP[m.group(2)],
            "value": float(m.group(1))
        }

    # Comparison without variable: >=150
    m = re.fullmatch(r"(<=|>=|<|>|==)(-?\d+(?:\.\d+)?)", expr)
    if m:
        return {
            "type": "comparison",
            "op": m.group(1),
            "value": float(m.group(2))
        }

    # Single number → equality
    m = re.fullmatch(r"-?\d+(?:\.\d+)?", expr)
    if m:
        return {
            "type": "comparison",
            "op": "==",
            "value": float(expr)
        }

    raise ValueError(f"Invalid expression: {expr}")


def matches_filter_condition(condition_dict, x):

    OPS = {
        "<":  operator.lt,
        "<=": operator.le,
        ">":  operator.gt,
        ">=": operator.ge,
        "==": operator.eq
    }

    if condition_dict["type"] == "range":
        return condition_dict["min"] < x < condition_dict["max"]
    return OPS[condition_dict["op"]](x, condition_dict["value"])



if __name__ == "__main__":

    csv_path = '/home/.../aqua90m/test_input_data/spdata.csv'
    # NOT COMMIT:
    csv_path = '/home/mbuurman/work/repos_and_scripts/aqua90mmm/aqua90m/test_input_data/spdata.csv'
    input_df = pd.read_csv(csv_path)

    # Filter by attribute
    keep_attribute = "site_id"
    keep_values = [1, 10, 20]
    print(f'Keep {keep_attribute}: {keep_values}')
    out_df = filter_dataframe(input_df, keep_attribute, keep_values)
    print('OUT: %s' % out_df)
    #out_df.to_csv('./test_filtered_data1.csv', index=False)

    # Filter by condition
    keep_attribute = "latitude"
    condition = "x==-18.5"
    condition_dict = parse_filter_condition(condition, var="x")
    print(f'Keep {keep_attribute} ({condition}): {condition_dict}')
    out_df = filter_dataframe_by_condition(input_df, keep_attribute, condition_dict)
    print('OUT: %s' % out_df)
    #out_df.to_csv('./test_filtered_data1.csv', index=False)

    # Filter by condition
    keep_attribute = "latitude"
    condition = "x>=-12"
    condition_dict = parse_filter_condition(condition, var="x")
    print(f'Keep {keep_attribute} ({condition}): {condition_dict}')
    out_df = filter_dataframe_by_condition(input_df, keep_attribute, condition_dict)
    print('OUT: %s' % out_df)
    #out_df.to_csv('./test_filtered_data2.csv', index=False)

    # Filter by condition
    keep_attribute = "longitude"
    condition = "-45<x<-44"
    condition_dict = parse_filter_condition(condition, var="x")
    print(f'Keep {keep_attribute} ({condition}): {condition_dict}')
    out_df = filter_dataframe_by_condition(input_df, keep_attribute, condition_dict)
    print('OUT: %s' % out_df)
    #out_df.to_csv('./test_filtered_data3.csv', index=False)

    # Filter one after the other:
    keep_attribute = "latitude"
    condition = "x>=-17"
    condition_dict = parse_filter_condition(condition, var="x")
    print(f'Keep {keep_attribute} ({condition}): {condition_dict}')
    out_df2 = filter_dataframe_by_condition(out_df, keep_attribute, condition_dict)
    print('OUT 2: %s' % out_df2)

    # Filter one after the other:
    # Try the other way around:
    keep_attribute = "latitude"
    condition = "-17<=x"
    condition_dict = parse_filter_condition(condition, var="x")
    print(f'Keep {keep_attribute} ({condition}): {condition_dict}')
    out_df2 = filter_dataframe_by_condition(out_df, keep_attribute, condition_dict)
    print('OUT 2: %s' % out_df2)
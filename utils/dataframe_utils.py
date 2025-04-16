import pandas as pd

def filter_dataframe(input_df, keep_attribute, keep_values):

    everything = []

    # Iterate over all rows:
    for row in input_df.itertuples(index=False):
        curr_value = getattr(row, keep_attribute)
        if curr_value in keep_values:
            # keep!!
            # Collect results in list:
            everything.append(row)

    # Finished collecting the results, now make pandas dataframe:
    dataframe = pd.DataFrame(everything)
    return dataframe


if __name__ == "__main__":

    csv_path = '/home/.../aqua90m/spdata.csv'
    # NOT COMMIT:
    csv_path = '/home/mbuurman/work/repos_and_scripts/aqua90mmm/aqua90m/spdata.csv'
    input_df = pd.read_csv(csv_path)
    keep_attribute = "site_id"
    keep_values = [1, 10, 20]
    out_df = filter_dataframe(input_df, keep_attribute, keep_values)
    print('OUT: %s' % out_df)
    out_df.to_csv('./test_filtered_data.csv', index=False)

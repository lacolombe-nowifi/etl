'''
This script parses and cleans Divvy RSS feed data collected using `scrape_divvy_rss_feed.py`.
The resulting tabular data is then compressed and saved in Parquet format.
'''

# Load packages
import os
import re
import ast
import gzip
import pandas as pd
import fastparquet

# Specify directory paths to use
dir_raw = './Raw_Data/'
dir_cooked = './Processed_Data/'

# Write a function to parse bytes of each snapshot into a list of dictionaries
def parse_byt_to_lst(byt):
    string = byt.decode('utf-8').strip().replace('false', 'False').replace('true', 'True')
    lst = ast.literal_eval(string)
    for x in lst[1:]:
        x['timestamp'] = lst[0]  # Put timestamp
    return lst[1:]

# Write a function to parse and transform the raw data into a DataFrame
def transform_into_df(raw_gz_str):
    # Open and read raw gzip data
    with gzip.open(raw_gz_str, 'rb') as f:
        content = f.readlines()
    
    # Transform data into a DataFrame
    lst_of_lst = list(map(parse_byt_to_lst, content))  # Parse into a list of dictionaries
    lst_of_dict = [item for lst in lst_of_lst for item in lst]  # Flatten into one list
    df = pd.DataFrame(lst_of_dict)
    
    return df

# Parse and transform unprocessed raw data
if __name__ == '__main__':
    for fname_raw in os.listdir(dir_raw):
        if bool(re.match('divvy_\d\d\d\d-\d\d-\d\d.txt.gz', fname_raw)):  # Check if the file is the expected raw data
            fname_cooked = fname_raw.replace('.txt.gz', '.gzip')
            if fname_cooked not in os.listdir(dir_cooked):  # To skip files that have been already processed
                try:
                    df_day = transform_into_df(dir_raw + fname_raw)
                    df_day.to_parquet(dir_cooked + fname_cooked, compression='gzip')  # Compress and save
                except:
                    print(fname_raw)
                    pass

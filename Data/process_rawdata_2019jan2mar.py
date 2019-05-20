'''
This script cleans and combines Divvy RSS feed data that Neil's 
original scraping code collected between 01/31/19 and 03/22/19.
'''

# Load packages
import os
import re
import pandas as pd
import fastparquet

# Read in log file
with open("./Raw_Data/nohup.out") as f:
    log = f.readlines()
log = [x.strip().split(" ") for x in log] # Remove "\n"; split by space
log[0][2] = "update"; log[0].append("initial") # First element records initial snapshot; make format consistent

# Write a function to load and clean raw CSV data
def clean_raw(raw_csv_str):
    # Specify the date of the data to be cleaned
    datadate = raw_csv_str.replace("divvy_", "").replace(".csv", "")

    # Read in data
    with open("./Raw_Data/" + raw_csv_str) as f:
        content = f.readlines()
    content = [x.strip().split(",") for x in content] # Remove "\n"; split by comma

    # Get the list of time stamps with nonzero updates
    timestamp = [] # Initialize list
    for x in log:
        if (x[0] == datadate) and (x[3] != "0"):
            timestamp.append(x[1])

    # Merge in proper date and time information
    lst = [] # Initialize list to save processed objects
    colnames = content[0] # Save column names
    idx = -1 # To count from zero in the loop
    for x in content:
        if x[0] == colnames[0]: # For rows with column names
            idx += 1
        elif len(x) == len(colnames): # For rows with the expected number of data points
            y = x + [datadate, timestamp[idx]]
            lst.append(y)
        else: # For rows without the expected number of data points
            y = [None]*len(colnames) + [datadate, timestamp[idx]]
            lst.append(y)
    colnames_ext = colnames + ["Date", "Time"] # Add new column names
    df = pd.DataFrame(lst, columns = colnames_ext) # Transform into a DataFrame
    return df

# Clean and combine all data
fnames = os.listdir("./Raw_Data/") # Get the list of files in the data directory
lst_all = [] # Initialize list to save DataFrames
for fn in fnames:
    if bool(re.match("divvy_\d\d\d\d-\d\d-\d\d.csv", fn)): # Check if the file is the expected data file
        try:
            df_day = clean_raw(fn)
            lst_all.append(df_day)
        except:
            print(fn)
            pass
df_all = pd.concat(lst_all) # Combine into one DataFrame
df_all.to_parquet("./Processed_Data/divvy_2019jan2mar.gzip", compression="gzip") # Compress and save

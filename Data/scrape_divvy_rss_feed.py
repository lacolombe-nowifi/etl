'''
This script scrapes Divvy RSS feed data in every 30 seconds and saves station status updates into CSV files.
'''

# Load packages
import csv
import requests
import time
from datetime import datetime
import logging
import gzip
import json
import os

# Define variables to use
divvy_url = "https://feeds.divvybikes.com/stations/stations.json" # URL for Divvy RSS feed data
dir_path = "./Raw_Data/" # Specify directory to save the data
interval = 30 # Time interval in seconds

# Write a function to save a list of dictionaries into a CSV file (deprecated)
def save_dict_lst(fpath, dict_lst):
    with open(fpath, "a") as f:
        keys = dict_lst[0].keys()
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writeheader()
        dict_writer.writerows(dict_lst)

# save list as txt.gzip file
def append_write_gzip(delta_lst, fpath):
    if os.path.exists(fpath):
        append_write = 'at'  # Append if already exists
    else:
        append_write = 'wt'  # Make a new file if not

    with gzip.open(fpath, append_write) as f:
        json.dump(delta_lst, f)
        f.write('\n')
    return(True)

# Write a function that compares current vs. previous "snapshots" to identify stations with changed status
def get_lst_diff(lst_curr, lst_prev):
    lst_diff = []
    for curr_x in lst_curr:
        prev_x = [prev_x for prev_x in lst_prev if prev_x['id'] == curr_x['id']][0] # Identify the same station
        if prev_x:  # If id existed before
            if curr_x['availableBikes'] != prev_x['availableBikes']:
                lst_diff.append(curr_x)
        else:       # If new stations appears
            lst_diff.append(curr_x)
    return(lst_diff)

# Collect data in every 30 seconds
if __name__ == '__main__':
    logging.basicConfig(filename='scrape_log.txt', level=logging.INFO)  # Start scraping log
    while True:
        scrape_ts = datetime.utcnow() # Put timestamp info in UTC
        file_path = f"divvy_{scrape_ts.strftime('%Y-%m-%d')}.txt.gz"
        scrape_ts_str = scrape_ts.isoformat()
    
        try:
            req = requests.get(divvy_url).json()['stationBeanList'] # Get RSS feed data
            try:
                prev # Check if previous data exists; False only for the first iteration
                diff = get_lst_diff(req, prev) # Identify stations with changed status
                if len(diff) > 0:
                    append_write_gzip([scrape_ts_str] + diff, file_path) # Save data for stations with changed status
                    logging.info(f"{scrape_ts_str + ' UTC'}: Delta {len(diff)}")
                prev = req # For the next iteration
            except: # If previous data does not exist (i.e. for the first iteration)
                append_write_gzip([scrape_ts_str] + req, file_path) # Save initial "snapshot"
                prev = req # For the next iteration
                logging.info(f"{scrape_ts_str + ' UTC'}: Initial {len(req)}")
        except:
            pass
        time.sleep(interval)
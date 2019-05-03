'''
This script scrapes Divvy RSS feed data in every 30 seconds and saves station status updates into CSV files.
'''

# Load packages
import csv
import requests
import time
from datetime import datetime

# Define variables to use
divvy_url = "https://feeds.divvybikes.com/stations/stations.json" # URL for Divvy RSS feed data
dir_path = "./Raw_Data/" # Specify directory to save the data
interval = 30 # Time interval in seconds

# Write a function to save a list of dictionaries into a CSV file
def save_dict_lst(fpath, dict_lst):
    with open(fpath, "a") as f:
        keys = dict_lst[0].keys()
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writeheader()
        dict_writer.writerows(dict_lst)

# Write a function that compares current vs. previous "snapshots" to identify stations with changed status
def get_lst_diff(lst_curr, lst_prev):
    lst_diff = []
    for curr_x in lst_curr:
        prev_x = [prev_x for prev_x in lst_prev if prev_x['id'] == curr_x['id']][0] # Identify the same station
        if curr_x['availableBikes'] != prev_x['availableBikes']:
            lst_diff.append(curr_x)
    return lst_diff

# Collect data in every 30 seconds
if __name__ == '__main__':
    while True:
        try:
            req = requests.get(divvy_url).json()['stationBeanList'] # Get RSS feed data
            for x in req:
                x['timestamp'] = datetime.now().isoformat() # Put timestamp info
            try:
                prev # Check if previous data exists; False only for the first iteration
                diff = get_lst_diff(req, prev) # Identify stations with changed status
                if len(diff) > 0:
                    file_path = dir_path + f"divvy_{datetime.now().strftime('%Y-%m-%d')}.csv"
                    save_dict_lst(file_path, diff) # Save data for stations with changed status
                prev = req # For the next iteration
            except: # If previous data does not exist (i.e. for the first iteration)
                file_path = dir_path + f"divvy_{datetime.now().strftime('%Y-%m-%d')}.csv"
                save_dict_lst(file_path, req) # Save initial "snapshot"
                prev = req # For the next iteration
        except:
            pass
        time.sleep(interval)

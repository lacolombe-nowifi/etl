import csv
import requests
import time
from datetime import datetime

URL = 'https://feeds.divvybikes.com/stations/stations.json'
INTERVAL = 30   # in seconds

def stream_dict_save(file, dict_lst):
    with open(file, "a") as f:
        keys = dict_lst[0].keys()
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writeheader()
        dict_writer.writerows(dict_lst)

def comp_dict_const(dict_lst):
    return({e['id']: e['availableBikes'] for e in dict_lst})

def comp_dict(dict_base, dict_comp):
    return([k for k, v in enumerate(dict_comp.items()) 
            if v[1] != dict_base.get(v[0]) 
            or dict_base.get(v[0]) is None])

if __name__ == '__main__':
    while True:
        try:
            req = requests.get(URL).json()['stationBeanList']
            try:
                prev
                prev_comp = comp_dict_const(prev)
                req_comp = comp_dict_const(req)
                update_idx = comp_dict(prev_comp, req_comp)
            
                if update_idx:
                    update_val = [v for k, v in enumerate(req) if k in update_idx]
                    stream_dict_save(f"data/divvy/divvy_{datetime.now().strftime('%Y-%m-%d')}.csv", update_val)
                prev = req
                print(f"{datetime.now()} update: {len(update_idx)}")

            except:
                stream_dict_save(f"data/divvy/divvy_{datetime.now().strftime('%Y-%m-%d')}.csv", req)

                prev = req
                print(f"{datetime.now()} initial")
        except:
            pass
        time.sleep(INTERVAL)

import os, json, requests
import pandas as pd
import python_cde.cdeconnection as cde
from datetime import datetime as dt
import pytz
import requests
import subprocess as sp
#import yagmail
import sys

import warnings
warnings.filterwarnings('ignore')

import matplotlib.pyplot as plt
import numpy as np
import itertools
import seaborn as sns
import matplotlib.colors as colors
import matplotlib.cm as cmx
from matplotlib.pyplot import figure

# Setting variables for script
JOBS_API_URL = "https://74jgph6m.cde-gkhphnpj.ankity-a.a465-9q4k.cloudera.site/dex/api/v1"
WORKLOAD_USER = "ankity" #"cdpusername"
WORKLOAD_PASSWORD = "Welcome1$" #"cdppwd"

# Instantiate the Connection to CDE
cde_connection = cde.CdeConnection(JOBS_API_URL, WORKLOAD_USER, WORKLOAD_PASSWORD)
TOKEN = cde_connection.set_cde_token()

#headers for API Call
headers = {
    'Authroization': f"Bearer {TOKEN}",
    'accept': 'application/json',
    'Content-Type': 'application/json'
}

def get_data(offset_start,batches):
    runs = list(range(offset_start, (batches*100)+ offset_start, 100))
    raw_data = pd.DataFrame()
    for offset in runs:
        output = sp.getoutput(f'curl -s -H "Authorization: Bearer {TOKEN}" -H "Content-Type: application/json" -X GET "https://74jgph6m.cde-gkhphnpj.ankity-a.a465-9q4k.cloudera.site/dex/api/v1/job-runs?offset={offset}&limit=100"')
        convert_to_json = json.loads(output)
        json_df = pd.DataFrame(convert_to_json["runs"])
        raw_data = raw_data.append(json_df, ignore_index=True)
    return raw_data

offset_start = 1 #Jobs will be retrieved starting from this Job Run ID
batches = 1 # Number*100 of jobs will be fetched, e.g. When value is 4 -> 4*100 = 400 jobs

#TODO Should only take Delta

raw_data = get_data(offset_start, batches)

raw_data.to_csv("dataset/sandbox_cde.csv", index=False)
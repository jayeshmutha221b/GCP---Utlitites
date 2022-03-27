import pandas as pd
import json
import requests
from pandas.io import gbq
import pandas_gbq
import json,math
import datetime as dt
import time
import numpy as np

# Get data from API
def get_api_data(request):
  time.sleep(10)
  request_json = request.get_json()
  pair = ["ZRX-USD","1INCH-USD","AAVE-USD","BTC-USD","DOGE-USD","ETH-USD","ETC-USD"]
  epoch_endDate = dt.datetime.now().timestamp()
  epoch_startDate = epoch_endDate-300
  for p in pair:
      url_startDate = dt.datetime.utcfromtimestamp(epoch_startDate).strftime('%Y-%m-%dT%H:%M:%SZ')
      url_endDate = dt.datetime.utcfromtimestamp(epoch_endDate).strftime('%Y-%m-%dT%H:%M:%SZ')
      url = f'https://api.pro.coinbase.com/products/{p}/candles?start={url_startDate}&end={url_endDate}&granularity=300'
      # Get response from url
      response = requests.get(url)
      if response.status_code == 200:  # check to make sure the response from server is good
            data = pd.DataFrame(json.loads(response.text), columns=['unix', 'low', 'high', 'open', 'close', 'volume'])
            data['date'] = pd.to_datetime(data['unix'], unit='s')  # convert to a readable date
            data['pair'] = p
            bq_load(p, data)
 
'''
This function just converts your pandas dataframe into a bigquery table, 
'''
  
def bq_load(key, value):
  
  project_name = 'dataproc-poc-314806'
  dataset_name = 'Coinbase'
  table_name = key
  
  value.to_gbq(destination_table='{}.{}'.format(dataset_name, table_name), project_id=project_name, if_exists='append',
  table_schema=[{'name': 'unix','type': 'INTEGER'}, {'name': 'low','type': 'FLOAT'}, {'name': 'high','type': 'FLOAT'},
   {'name': 'open','type': 'FLOAT'}, {'name': 'close','type': 'FLOAT'}, {'name': 'volume','type': 'FLOAT'}, 
   {'name': 'date','type': 'DATETIME'}])
  

""" import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import json

with open("data.json", 'r') as file:
    json_data = json.load(file) #dict

 """
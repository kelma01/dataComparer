import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import json
import xmltodict
import fastavro
from fastavro.schema import load_schema

#read json file
with open('data.json', 'r') as json_file:
    json_data = json.load(json_file)

#converting json format to parquet format
df = pd.read_json('data.json')
table = pa.Table.from_pandas(df)
pq.write_table(table, 'data.parquet')


#converting json format to xml format
xml_data = xmltodict.unparse({"root": {"item": json_data}}, pretty=True)
with open('data.xml', 'w', encoding='utf-8') as xml_file:
    xml_file.write(xml_data)

#converting json format to avro format
schema = {
  "type": "record",
  "name": "RecordSchema",
  "fields": [
    {
      "name": "sid",
      "type": "string"
    },
    {
      "name": "id",
      "type": "string"
    },
    {
      "name": "position",
      "type": "int"
    },
    {
      "name": "created_at",
      "type": "long"
    },
    {
      "name": "created_meta",
      "type": "string"
    },
    {
      "name": "meta",
      "type": "string"
    },
    {
      "name": "Unique ID",
      "type": "string"
    },
    {
      "name": "Indicator ID",
      "type": "string"
    },
    {
      "name": "Name",
      "type": "string"
    },
    {
      "name": "Measure",
      "type": "string"
    },
    {
      "name": "Measure Info",
      "type": "string"
    },
    {
      "name": "Geo Type Name",
      "type": "string"
    },
    {
      "name": "Geo Join ID",
      "type": "string"
    },
    {
      "name": "Geo Place Name",
      "type": "string"
    },
    {
      "name": "Time Period",
      "type": "string"
    },
    {
      "name": "Start_Date",
      "type": "string"
    },
    {
      "name": "Data Value",
      "type": "string"
    },
    {
      "name": "Message",
      "type": ["null", "string"]
    }
  ]
}

with open('data.avro', 'wb') as out_file:
    fastavro.writer(out_file, schema, json_data)

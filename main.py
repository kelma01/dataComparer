import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import json
import xmltodict
import fastavro
from fastavro.schema import load_schema

#read json file
with open('data2.json', 'r') as json_file:
    json_data = json.load(json_file)

#converting json format to parquet format
df = pd.read_json('data2.json')
table = pa.Table.from_pandas(df)
pq.write_table(table, 'data2.parquet')


#converting json format to xml format
xml_data = xmltodict.unparse({"root": {"item": json_data}}, pretty=True)
with open('data2.xml', 'w', encoding='utf-8') as xml_file:
    xml_file.write(xml_data)

#converting json format to avro format
schema = {
    "type": "record",
    "name": "NewsArticle",
    "fields": [
        {"name": "link", "type": "string"},
        {"name": "headline", "type": "string"},
        {"name": "category", "type": "string"},
        {"name": "short_description", "type": "string"},
        {"name": "authors", "type": "string"},
        {"name": "date", "type": "string"}
    ]
}
with open('data2.avro', 'wb') as out_file:
    fastavro.writer(out_file, schema, json_data)

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import xmltodict
import fastavro

#read json file
with open('datasets/data.json', 'r') as json_file:
    json_data = json.load(json_file)

#converting json format to parquet format
df = pd.read_json('datasets/data.json')
table = pa.Table.from_pandas(df)
pq.write_table(table, 'datasets/data.parquet')

table = pq.read_table('datasets/data.parquet')
new_fields = []
for field in table.schema:
    if field.type == pa.timestamp("ns"):  # 
        new_fields.append(pa.field(field.name, pa.timestamp("ms"))) #NANOS formatina ait timestampler pyspark tarafindan okunamiyor dolayisi ile ms'ye convert ediliyor
    else:
        new_fields.append(field) 

new_schema = pa.schema(new_fields)
new_table = table.cast(new_schema)
pq.write_table(new_table, "datasets/data.parquet")  # schema parametresi kaldırıldı! """




#converting json format to xml format
xml_data = xmltodict.unparse({"root": {"item": json_data}}, pretty=True)
with open('datasets/data.xml', 'w', encoding='utf-8') as xml_file:
    xml_file.write(xml_data)

#converting json format to avro format
schema = {
  "type": "record",
  "name": "avroSchema",
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
      "name": "Unique_ID",
      "type": "string"
    },
    {
      "name": "Indicator_ID",
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
      "name": "Measure_Info",
      "type": "string"
    },
    {
      "name": "Geo_Type_Name",
      "type": "string"
    },
    {
      "name": "Geo_Join_ID",
      "type": "string"
    },
    {
      "name": "Geo_Place_Name",
      "type": "string"
    },
    {
      "name": "Time_Period",
      "type": "string"
    },
    {
      "name": "Start_Date",
      "type": "string"
    },
    {
      "name": "Data_Value",
      "type": "string"
    },
    {
      "name": "Message",
      "type": ["null", "string"]
    }
  ]
}

with open('datasets/data.avro', 'wb') as out_file:
    fastavro.writer(out_file, schema, json_data)

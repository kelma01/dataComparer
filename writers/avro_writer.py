import fastavro
import os
from datetime import datetime
import time
import psutil

start_time = time.time()
process = psutil.Process()

schema = {
    "type": "record",
    "name": "AvroSchema",
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

date_str = "2014-06-01T00:00:00"
dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
timestamp_ms = dt.timestamp() * 1000  # 

new_record = {
    "sid": "row-test-test-test",
    "id": "00000000-0000-0000-C8E0-8E315E3AF06C",
    "position": 0,
    "created_at": timestamp_ms,
    "created_meta": "null",
    "meta": "{ }",
    "Unique_ID": "221806",
    "Indicator_ID": "386",
    "Name": "Ozone (O3)",
    "Measure": "Mean",
    "Measure_Info": "ppb",
    "Geo_Type_Name": "UHF34",
    "Geo_Join_ID": "103",
    "Geo_Place_Name": "Fordham - Bronx Pk",
    "Time_Period": "Summer 2014",
    "Start_Date": "2014-06-01T00:00:00",
    "Data_Value": "30.7",
    "Message": "0.1"
}

avro_file = "datasets/data.avro"

if os.path.exists(avro_file):
    with open(avro_file, "rb") as f:
        existing_records = [record for record in fastavro.reader(f)]
else:
    existing_records = []

existing_records.append(new_record)

with open(avro_file, "wb") as out:
    fastavro.writer(out, schema, existing_records)


print(f"========================================================================")
print(f"Writing Time of Avro formatted file: {time.time() - start_time} seconds")
print(f"Memory Usage After Writing: {process.memory_info().rss / (1024 * 1024):.2f} MB")
print(f"========================================================================")


#execute this line for running: `python .\writers\avro_writer.py`
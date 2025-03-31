import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import time
import psutil

start_time = time.time()
process = psutil.Process()

date_str = "2014-06-01T00:00:00"
dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")

timestamp_ms = int(dt.timestamp() * 1000)

new_data = pa.Table.from_pandas(pd.DataFrame([{
    "sid": "row-test-test-tes3",
    "id": "00000000-0000-0000-C8E0-8E315E3AF06C",
    "position": 0,
    "created_at": pa.array([timestamp_ms], type=pa.timestamp('ms'))[0],  #timestamp verileri uyussun diye
    "created_meta": "null",
    "meta": "{ }",
    "Unique_ID": 221806,
    "Indicator_ID": 386,
    "Name": "Ozone (O3)",
    "Measure": "Mean",
    "Measure_Info": "ppb",
    "Geo_Type_Name": "UHF34",
    "Geo_Join_ID": 103,
    "Geo_Place_Name": "Fordham - Bronx Pk",
    "Time_Period": "Summer 2014",
    "Start_Date": "2014-06-01T00:00:00",
    "Data_Value": 30.7,
    "Message": 0.1
}]))

try:
    existing_table = pq.read_table("datasets/data.parquet")
    combined_table = pa.concat_tables([existing_table, new_data])
except FileNotFoundError:
    combined_table = new_data

pq.write_table(combined_table, "datasets/data.parquet")

print(f"========================================================================")
print(f"Writing Time of Parquet formatted file: {time.time() - start_time} seconds")
print(f"Memory Usage After Writing: {process.memory_info().rss / (1024 * 1024):.2f} MB")
print(f"========================================================================")


#execute this line for running: `python .\writers\parquet_writer.py`
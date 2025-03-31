import time
import json
import psutil
from pyspark.sql import SparkSession

start_time = time.time()
process = psutil.Process()

spark = SparkSession.builder \
    .appName("HDFS JSON Read") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://172.27.90.91:9000") \
    .config("spark.hadoop.dfs.client.read.shortcircuit", "false") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()
 
df = spark.read.json("hdfs://172.27.90.91:9000/user/kerem/datasets/data.json")
new_entry = spark.createDataFrame([{
    "sid": "row-test-test-tes5",
    "id": "00000000-0000-0000-C8E0-8E315E3AF06C",
    "position": 0,
    "created_at": 1712774929,
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
    "Message": "null"}])

df_updated = df.union(new_entry)
df_updated.write.mode("overwrite").json("hdfs://172.27.90.91:9000/user/kerem/datasets/data.json")

end_time = time.time()
memory_usage = process.memory_info().rss / (1024 * 1024) 

print(f"========================================================================")
print(f"Writing Time of JSON formatted file: {end_time - start_time} seconds")
print(f"Memory Usage After Writing: {memory_usage:.2f} MB")
print(f"========================================================================")


#execute this line for running: `python .\writers\json_writer.py` """
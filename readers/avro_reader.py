import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Avro Reader").getOrCreate()

start_time = time.time()

df = spark.read.format("avro").load("datasets/data.avro")

end_time = time.time()

reading_time = end_time - start_time
print(f"========================================================================\nReading Time of Avro formatted file: {reading_time} seconds\n========================================================================")

#execute line: `spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.4 .\readers\avro_reader.py`
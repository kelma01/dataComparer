import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Json Reader").getOrCreate()

start_time = time.time()

df = spark.read.json("datasets/data.json")

end_time = time.time()

reading_time = end_time - start_time
print(f"========================================================================\nJSON formatlı dosyanın okuma süresi: {reading_time} saniye\n========================================================================")

#execute this line for running: `spark-submit .\readers\json_reader.py`

import time
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("XML Reader").getOrCreate()

start_time = time.time()

df = spark.read.format('com.databricks.spark.xml').option("root", "item").load("datasets/data.xml")

end_time = time.time()

reading_time = end_time - start_time
print(f"========================================================================\nReading Time of XML formatted file: {reading_time} seconds\n========================================================================")

#execute this line for running: `spark-submit --packages com.databricks:spark-xml_2.12:0.14.0 .\readers\xml_reader.py`

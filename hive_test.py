from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("DataComparerWithHive") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()


json_path = "datasets/sample.json"

start = time.time()

df = spark.read.json(json_path)

df.write.mode("overwrite").saveAsTable("json_table_test")

result = spark.sql("SELECT * FROM json_table_test LIMIT 10")
result.show()

end = time.time()
print(f"İşlem süresi: {end - start:.2f} saniye")

spark.stop()

from pyspark.sql import SparkSession
import time

# SparkSession - Hive destekli
spark = SparkSession.builder \
    .appName("DataComparerWithHive") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

# JSON dosyasının yolu
json_path = "data/sample.json"  # Dosyayı buraya koyabilirsin

# Süreyi ölç
start = time.time()

# JSON verisini oku
df = spark.read.json(json_path)

# Hive tablosu olarak kaydet
df.write.mode("overwrite").saveAsTable("json_table_test")

# Sorgu örneği
result = spark.sql("SELECT * FROM json_table_test LIMIT 10")
result.show()

end = time.time()
print(f"İşlem süresi: {end - start:.2f} saniye")

spark.stop()

from pyspark.sql import SparkSession
import time
import os

spark = SparkSession.builder \
    .appName("DataComparerHiveTest") \
    .enableHiveSupport() \
    .getOrCreate()

DATA_DIR = "datasets"

files = [
    "data.avro",
    "data.json",
    "data.parquet",
    "data.xml",
]

def read_file(file_path, ext):
    if ext == ".json":
        return spark.read.json(file_path)
    elif ext == ".parquet":
        return spark.read.parquet(file_path)
    elif ext == ".avro":
        return spark.read.format("avro").load(file_path)
    elif ext == ".xml":
        return spark.read.format("xml").option("rowTag", "root").load(file_path)  # rowTag ihtiyaca göre değişir
    else:
        raise Exception(f"Desteklenmeyen format: {ext}")

for file in files:
    file_path = os.path.join(DATA_DIR, file)
    name = os.path.splitext(file)[0]
    ext = os.path.splitext(file)[1]

    print(f"\n{file} dosyası işleniyor...")

    try:
        start = time.time()

        df = read_file(file_path, ext)
        df.printSchema()

        table_name = f"hive_{name}"
        df.write.mode("overwrite").saveAsTable(table_name)

        spark.sql(f"SELECT * FROM {table_name} LIMIT 10").show()

        end = time.time()
        print(f"{file} başarıyla işlendi. Süre: {end - start:.2f} saniye")

    except Exception as e:
        print(f"{file} dosyası işlenirken hata oluştu: {e}")

spark.stop()

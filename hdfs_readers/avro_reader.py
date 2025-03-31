import time
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HDFS Avro Read") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://172.27.90.91:9000") \
    .config("spark.hadoop.dfs.client.read.shortcircuit", "false") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

jvm = spark._jvm
runtime = jvm.java.lang.Runtime.getRuntime()
memory_before = runtime.totalMemory() - runtime.freeMemory()

start_time = time.time()

df = spark.read.format("avro").load("hdfs://172.27.90.91:9000/user/kerem/datasets/data.avro")

end_time = time.time()

df.show()

memory_after = runtime.totalMemory() - runtime.freeMemory()
reading_time = end_time - start_time

partition_number = df.rdd.getNumPartitions()
def_parallelism = spark.sparkContext.defaultParallelism
exec_instances = spark.sparkContext.getConf().get('spark.executor.instances', "2")

print(f"========================================================================")
print(f"Reading Time of Avro formatted file: {reading_time} seconds")
print(f"Memory Usage Before Reading: {memory_before / (1024 * 1024):.2f} MB")
print(f"Memory Usage After Reading: {memory_after / (1024 * 1024):.2f} MB")
print(f"========================================================================")
# bu satirin ustundeki info read rate ve memo usage ile alakali
# bu satirin alti ise parallelism ve partition sayisi ile alakali
print(f"Parallelism, Executors, Core numbers information about Avro formatted file:")
print(f"Partition Count: {partition_number}")
print(f"Default Parallelism: {def_parallelism}")
print(f"Executor Instances: {exec_instances}")
print(f"========================================================================")

#execute line: `spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.4 .\hdfs_readers\avro_reader.py`
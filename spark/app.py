from pyspark.sql import SparkSession
from time import sleep

spark = SparkSession.builder.appName("k8s-test").getOrCreate()

df = spark.range(1000)
sleep(120)
print(df.count())

spark.stop()
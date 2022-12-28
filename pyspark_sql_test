from pyspark.sql.functions import *
from pyspark.sql import SparkSession

filename = 'test.csv'
spark = SparkSession.builder \
.master("local") \
.appName("app") \
.getOrCreate()

data = spark.read.csv(filename, header=True, inferSchema=True, sep=';')
data.show()

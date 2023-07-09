from pyspark.sql import functions as F
from pyspark.sql import SparkSession

filename = 'test.csv'

spark = SparkSession.builder \
    .master("local") \
    .appName("app") \
    .getOrCreate()

data = spark.read.csv(filename, header=True, inferSchema=True, sep=',')
data.show()

name = data.select(
    "date",
    F.col("name").alias("Name")
)
name.show()

print(name.collect())

data.createOrReplaceTempView('users')
df = spark.sql('SELECT *, TRUE as spark_user FROM users').drop('score')
df.show()

sum_table = data.groupBy("name").sum("score")
sum_table.show()

l = [('Alice', 1)]
T = spark.createDataFrame(l, ['name', 'age']).collect()
print(T)

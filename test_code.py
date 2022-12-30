# from pyspark.sql.functions import * と書く事も多い。
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


filename = 'test.csv'
spark = SparkSession.builder \
.master("local") \
.appName("app") \
.getOrCreate()

# sepを；にすると、全ての列が一つにまとめられてしまう。
data = spark.read.csv(filename, header=True, inferSchema=True, sep=',')
data.show()

name = data.select(
    "date",
    F.col("name").alias("Name")
)
name.show()


data.registerTempTable('users')
df = spark.sql('SELECT * FROM users').withColumn('spark_user', F.lit(True))
df.show()




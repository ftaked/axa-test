from pyspark.sql import SparkSession
from pyspark.sql.functions import asc

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("how to read csv file") \
    .getOrCreate()

df = spark.read.csv('data_v1.csv', header=True, sep=',')
user_group_df = df.groupBy(df['User_ID']).agg()

user_group_df.count().show()

user_group_df

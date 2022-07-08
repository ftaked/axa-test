from pyspark.sql.functions import col
from pyspark.sql import SparkSession
SPARK = SparkSession.builder \
    .master("local[1]") \
    .appName("IAB") \
    .getOrCreate()

population_data = [{"國名": '日本', '人口数(百万人)': 126.3},
                   {"國名": 'アメリカ', '人口数(百万人)': 328.2},
                   {"國名": 'フランス', '人口数(百万人)': 67.06},
                   {"國名": 'ロシア', '人口数(百万人)': 144.4}]

population_df = SPARK.createDataFrame(population_data)
population_df.show()

population_avg_df = population_df.agg({"人口数(百万人)": "avg"})
population_avg_df = population_avg_df.select(
    col("avg(人口数(百万人))").alias("人口数の平均(百万人)"))
population_avg_df.show()

population_sum_df = population_df.agg({"人口数(百万人)": "sum"})
population_sum_df = population_sum_df.select(
    col("sum(人口数(百万人))").alias("人口数の総数(百万人)"))
population_sum_df.show()

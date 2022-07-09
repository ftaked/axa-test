from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import TimestampType, StructField, StructType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    asc,
    col,
    lag,
    when,
    lit,
    monotonically_increasing_id,
    floor,
    concat,
    approx_count_distinct,
    count,
    max,
)


def calc_session_number(diff_in_seconds_list):
    session_number = 0
    session_start_time = diff_in_seconds_list[0][0]
    session_number_list = []
    for i, diff in enumerate(diff_in_seconds_list):
        for key in diff.keys():
            if key >= 300:
                session_number = session_number + 1
                session_start_time = diff[key]
            session_number_list.append(
                {
                    "row_number": i,
                    "session_number": session_number,
                    "session_start_time": session_start_time,
                }
            )
    return session_number_list


def main():
    spark = SparkSession.builder.master("local").appName("axa test").getOrCreate()
    schema = StructType(
        [
            StructField("Action_time", TimestampType(), True),
            StructField("Category", StringType(), True),
            StructField("User_ID", StringType(), True),
            StructField("URL", StringType(), True),
        ]
    )
    df = spark.read.format("csv").option("header", True).schema(schema).load("example_data_2019_v1.csv")

    user_array = [list(x.asDict().values())[0] for x in df.select("User_ID").distinct().collect()]
    df_array = [df.where(df.User_ID == x) for x in user_array]

    for i in range(len(df_array)):
        df_array[i] = df_array[i].orderBy(asc("Action_time"))

        w = Window().partitionBy("User_ID").orderBy(col("Action_time"))
        df_array[i] = df_array[i].withColumn("lag_Action_time", lag("Action_time").over(w))
        df_array[i] = df_array[i].withColumn(
            "diff_in_seconds",
            when(df_array[i].lag_Action_time.isNull(), lit(0)).otherwise(
                col("Action_time").cast("long") - col("lag_Action_time").cast("long")
            ),
        )

        diff_in_seconds_list = [
            {list(x.asDict().values())[0]: list(x.asDict().values())[1]}
            for x in df_array[i].select("diff_in_seconds", "Action_time").collect()
        ]
        df_array[i] = df_array[i].withColumn("row_number", monotonically_increasing_id())
        session_number_df = spark.createDataFrame(calc_session_number(diff_in_seconds_list))

        df_array[i] = df_array[i].join(session_number_df, "row_number", "left")
        df_array[i] = df_array[i].orderBy("User_ID", "Action_time")

        df_session_number = (
            df_array[i]
            .withColumn("session_end_time", col("Action_time").cast("timestamp"))
            .groupBy("session_number")
            .agg(max("Action_time").alias("session_end_time"))
        )
        df_array[i] = df_array[i].join(df_session_number, "session_number", "left")

        df_array[i] = df_array[i].withColumn(
            "session_duration_seconds", col("session_end_time").cast("long") - col("session_start_time").cast("long")
        )
        df_array[i] = df_array[i].withColumn(
            "session_duration",
            concat(
                floor(col("session_duration_seconds") / 60) + 1,
                lit("分"),
                col("session_duration_seconds") % 60,
                lit("秒"),
            ),
        )

        w = Window().partitionBy("session_number")
        df_array[i] = df_array[i].withColumn("count_total_url", count("URL").over(w))
        df_array[i] = df_array[i].withColumn("count_unique_url", approx_count_distinct("URL").over(w))

    cols = (
        "URL",
        "diff_in_seconds",
        "session_duration_seconds",
        "Category",
        "row_number",
        "lag_Action_time",
        "Action_time",
        "session_end_time",
    )

    result_df = reduce(DataFrame.unionAll, df_array)
    result_df = result_df.drop(*cols).distinct().orderBy("User_ID", "Action_time")
    result_df.show(truncate=False, n=70)


if __name__ == "__main__":
    main()

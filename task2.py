from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, avg, col, sum as spark_sum, row_number
from pyspark.sql.window import Window

# 初始化Spark会话
spark = SparkSession.builder \
    .appName("Analysis") \
    .getOrCreate()

try:
    # 加载CSV文件为DataFrame
    transaction_df = spark.read.csv("user_balance_table.csv", header=True, inferSchema=True)
    user_info_df = spark.read.csv("user_profile_table.csv", header=True, inferSchema=True)

    # 检查数据加载是否成功
    if transaction_df.rdd.isEmpty() or user_info_df.rdd.isEmpty():
        raise ValueError("One of the data files is empty or not loaded correctly.")

    # 合并两个表
    data_df = transaction_df.join(user_info_df, on='user_id')

    # 将report_date列转换为日期类型
    data_df = data_df.withColumn("report_date", to_date(col("report_date").cast("string"), "yyyyMMdd"))

    # 任务2.1: 按城市统计2014年3月1日的平均余额
    march1_data = data_df.filter(col("report_date") == '2014-03-01')
    avg_balance_per_city = march1_data.groupBy("city").agg(avg("tBalance").alias("avg_tBalance")).orderBy(col("avg_tBalance").desc())
    avg_balance_per_city.show()
    avg_balance_per_city.write.mode("overwrite").csv("task2_result/1")

    # 任务2.2: 统计每个城市总流量前3高的用户
    august_data = data_df.filter((col("report_date") >= '2014-08-01') & (col("report_date") <= '2014-08-31'))
    total_flow_per_user_city = august_data.groupBy("city", "user_id").agg(
        spark_sum("total_purchase_amt").alias("total_purchase_amt"),
        spark_sum("total_redeem_amt").alias("total_redeem_amt")
    ).withColumn("total_flow", col("total_purchase_amt") + col("total_redeem_amt"))

    window_spec = Window.partitionBy("city").orderBy(col("total_flow").desc())
    ranked_users = total_flow_per_user_city.withColumn("rank", row_number().over(window_spec))
    top3_users_per_city = ranked_users.filter(col("rank") <= 3).select("city", "user_id", "total_flow", "rank").orderBy("city", "rank")
    top3_users_per_city.show()
    top3_users_per_city.write.mode("overwrite").csv("task2_result/2")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # 停止Spark会话
    spark.stop()
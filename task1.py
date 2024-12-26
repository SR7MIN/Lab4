from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# 初始化Spark会话
spark = SparkSession.builder \
    .appName("Calculation") \
    .getOrCreate()

# 加载CSV文件为DataFrame
file_path = "user_balance_table.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 任务1: 计算每日的资金流入和流出情况
rdd = df.rdd

# 映射每行数据到 (日期, (购买金额, 赎回金额))
mapped_rdd = rdd.map(lambda row: (row["report_date"], 
                                  (float(row["total_purchase_amt"] if row["total_purchase_amt"] is not None else 0), 
                                   float(row["total_redeem_amt"] if row["total_redeem_amt"] is not None else 0))))

# 按日期聚合资金流入和流出
aggregated_rdd = mapped_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# 按日期排序
sorted_rdd = aggregated_rdd.sortByKey()

# 格式化结果为 (日期, 流入, 流出)
result_rdd = sorted_rdd.map(lambda x: f"{x[0]}, {x[1][0]}, {x[1][1]}")

# 将结果保存到文本文件
result_rdd.saveAsTextFile("task1_result")

# 任务2: 统计2014年8月的活跃用户数量
# 过滤2014年8月的数据，并选择需要的列
filtered_df = df.filter((year(col("report_date")) == 2014) & (month(col("report_date")) == 8)) \
                .select("user_id", "report_date")

# 获取每个用户的活跃天数（去重）
user_active_days = filtered_df.distinct() \
                             .groupBy("user_id") \
                             .count() \
                             .withColumnRenamed("count", "active_days")

# 筛选出活跃天数大于等于5的用户
active_users = user_active_days.filter(col("active_days") >= 5)

# 计算活跃用户的数量
active_user_count = active_users.count()

print(f"Number of active users in August 2014: {active_user_count}")

# 停止Spark会话
spark.stop()



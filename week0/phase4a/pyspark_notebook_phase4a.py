from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
from pyspark.sql.functions import *
from pyspark.sql.window import Window


df1 = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
df2 = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

df_total = df2.groupBy("customer_id") \
    .agg(sum("total_amount").alias("total_spend"))


df = df1.join(df_total, on="customer_id", how="inner")

#1. Create Gold/Silver/Bronze segmentation using conditional logic
df = df.withColumn(
    "segment",
    when(df.total_spend > 100, "Gold")
    .when((df.total_spend >= 50) & (df.total_spend <= 100), "Silver")
    .otherwise("Bronze")
)

df.select("customer_id", "first_name", "total_spend", "segment").show()

#2. Group data by segment and count customers
df.groupBy("segment").count().show()

#3. Try quantile-based segmentation
quantiles = df.approxQuantile("total_spend", [0.33, 0.66], 0)

q1 = quantiles[0]
q2 = quantiles[1]

df = df.withColumn(
    "quantile_segment",
    when(df.total_spend <= q1, "Low")
    .when((df.total_spend > q1) & (df.total_spend <= q2), "Medium")
    .otherwise("High")
)

df.select("customer_id", "total_spend", "quantile_segment").show()


#4. Compare results of different methods
window = Window.orderBy("total_spend")

df = df.withColumn(
    "rank_pct",
    percent_rank().over(window)
)

df.select("customer_id", "total_spend", "rank_pct").show()

df.select(
    "customer_id",
    "total_spend",
    "segment",
    "quantile_segment",
    "rank_pct"
).show()

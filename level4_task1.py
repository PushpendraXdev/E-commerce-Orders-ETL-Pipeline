from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("xyz").getOrCreate()

customer = spark.read.csv("customers.csv",header=True,inferSchema=True)
order = spark.read.csv("orders.csv",header=True,inferSchema=True)

print("customers dataframe:")
customer.show()

print("orders dataframe:")
order.show()

total = customer.join(order,how="full",on="customer_id")

print(total.count())
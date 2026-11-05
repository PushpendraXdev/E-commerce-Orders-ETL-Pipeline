from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("xyz").getOrCreate()

customer = spark.read.csv("customers.csv",header=True,inferSchema=True)
order = spark.read.csv("orders.csv",header=True,inferSchema=True)

# task1 ****************************************************

# print("customers dataframe:")
# customer.show()

# print("orders dataframe:")
# order.show()

# total = customer.join(order,how="full",on="customer_id")

# print(total.count())

# task2 *****************************************************

invalid_quantity = order.filter(order["quantity"] <= 0)
missing_price = order.filter(order["price"].isNull())
duplicate_order_id = order.groupBy("order_id").agg(F.count("*").alias("row_count")).filter(F.col("row_count") > 1)
print("invalid quantity:")
invalid_quantity.show()

print("missing price:")
missing_price.show()

print("duplicate order id:")
duplicate_order_id.show()

spark.stop()
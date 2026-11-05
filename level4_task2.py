from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("xyz").getOrCreate()

customer = spark.read.csv("customers.csv",header=True,inferSchema=True)
order = spark.read.csv("orders.csv",header=True,inferSchema=True)

invalid_quantity = order.filter(order["quantity"] <= 0)
missing_price = order.filter(order["price"].isNull())
duplicate_order_id = order.groupBy("order_id").agg(F.count("*").alias("row_count")).filter(F.col("row_count") > 1)


validQ = order.filter(order['quantity'] > 0)
validP = order.filter(order['price'].isNotNull())
validU = order.drop_duplicates(['order_id'])
notincustomre = order.join(customer,on="customer_id",how="left_anti")
# validQ.show()

# validP.show()
# validU.show()

order_with_rejected_reason = order.withColumn("rejected_reason",F.when(F.col("quantity") <=0 , "Invalid Quantity").when(F.col("price").isNull(), "Missing Price").otherwise("Valid"))
order_with_rejected_reason.show() 

order_with_rejected_reason_2 = order.filter(F.col("customer_id").isNotNull())
spark.stop()

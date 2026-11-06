from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("xyz").getOrCreate()

customer = spark.read.csv("customers.csv",header=True,inferSchema=True)
order = spark.read.csv("orders.csv",header=True,inferSchema=True)
unique_orders = order.dropDuplicates(["order_id"])

unknown_customer= unique_orders.join(customer,how="left_anti",on="customer_id")

order_with_reason = unique_orders.withColumn("rejected_reason",
                                             F.when(
                                                 F.col('customer_id').isin(
                                                 [row['customer_id'] for row in unknown_customer.select("customer_id").collect()])
                                                 ,"Customer_Invalid")
                                             .when(F.col("quantity") <= 0,"Inavlid_Quantity").when(F.col("price").isNull(),"Missing Price")
.otherwise("Valid"))
valid_orders = order_with_reason.filter(
    F.col("rejected_reason") == "Valid"
)

rejected_orders = order_with_reason.filter(
    F.col("rejected_reason") != "Valid"
)

# valid_orders.show()
# rejected_orders.show()
duplicate_id = order.groupBy("order_id").count().filter(F.col("count") > 1)
ans = order.join(duplicate_id.select('order_id'),how="inner",on="order_id")
ans = ans.withColumn("rejected_reason",F.lit("Duplicate record"))

final_rejected = rejected_orders.unionByName(ans)
valid_orders = valid_orders.join(
    duplicate_id.select("order_id"),
    on="order_id",
    how="left_anti"
)

valid_orders = valid_orders.withColumn("revenue",valid_orders['price'] * valid_orders['quantity'])

# task 5 revenue with product

product_revenue = valid_orders.groupBy("product").agg(F.sum("revenue").alias("Total_revenue"))
product_revenue = product_revenue.orderBy(F.col("Total_revenue").desc())

# task 6 count summary pipeline

raw_count = order.count()
valid_count=  valid_orders.count()
invalid_count = final_rejected.count()
print(raw_count)
print(valid_count)
print(invalid_count)
spark.stop()


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

valid_orders.show()
rejected_orders.show()

spark.stop()


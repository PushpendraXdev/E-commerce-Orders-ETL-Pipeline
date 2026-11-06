import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sdcn").getOrCreate()

customers = spark.read.csv("customers.csv",header=True,inferSchema=True)
orders = spark.read.csv("orders.csv",header=True,inferSchema=True)

def extract_data():
    pass

def validate_data():
    pass

def transform_data():
    pass

def generate_report():
    pass

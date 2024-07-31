from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,IntegerType,FloatType

Spark = SparkSession.builder.appName("Customersorder").getOrCreate()


schema = StructType([
    StructField ("Customer_Id",IntegerType(),True),
    StructField("Item_ID",IntegerType(),True),
    StructField("Spent",FloatType(),True)
])

df = Spark.read.schema(schema).csv("/home/sai-murali/SparkCourse/customer-orders.csv")
df.show()
table = df.select("Customer_Id","Spent")
tan = table.groupBy("Customer_Id").agg(func.round(func.sum("Spent"),2).alias("toatal"))
tyu = tan.sort("toatal")
tyu.show(tyu.count())
Spark.stop()
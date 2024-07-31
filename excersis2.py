from pyspark.sql import SparkSession,Row
from pyspark.sql import functions as func
spark = SparkSession.builder.appName("fake_friends").getOrCreate()


##for loading the  without header file directly into df
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("/home/sai-murali/SparkCourse/fakefriends-header.csv")
    
people.select("age","friends").show()
people.groupBy("age").avg("friends").orderBy("age").show()
# Sorted
people.groupBy("age").avg("friends").sort("age").show()

# Formatted more nicely
people.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# With a custom column name
people.groupBy("age").agg(func.round(func.avg("friends"), 2)
  .alias("friends_avg")).sort("age").show()
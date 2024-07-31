from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,IntegerType,StringType
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("google_app_ratings").getOrCreate()

df = spark.read.csv("/home/sai-murali/SparkCourse/googleplaystore.csv",sep = ",",escape ='"',inferSchema = 'true',header = 'true')

##clean the data
t = df.drop("Size","Content Rating","Last Updated","Current Ver","Andriod Ver")

##modify
main_df = t.withColumn("Reviews",col("Reviews").cast(IntegerType())) \
    .withColumn("Installs",regexp_replace(col("Installs"),"[^0-9]",""))\
        .withColumn("Installs",col("Installs").cast(IntegerType()))\
             .withColumn("Price",regexp_replace(col("Price"),"[$]",""))\
                 .withColumn("Price",col("Price").cast(IntegerType()))


main_df.printSchema()

##clean
uyt = main_df.drop(col("Andriod Ver"))

##create a df 

uyt.createOrReplaceTempView("apps")

###write sql commands 
res = spark.sql('SELECT * FROM apps')


res.show()

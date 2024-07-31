from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,IntegerType,LongType,StringType

Spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

schema = StructType([
    StructField("user_ID",IntegerType(),True),
    StructField("Movie_ID",IntegerType(),True),
    StructField("Rating",IntegerType(),True),
    StructField("TimeStamp",LongType(),True),
])


df  =  Spark.read.option("sep","\t").schema(schema).csv("/home/sai-murali/SparkCourse/ml-100k/u.data")


sorted = df.groupBy("Movie_ID").count().orderBy(func.desc("count"))
sorted.show(10)
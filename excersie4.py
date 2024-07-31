from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import codecs
from pyspark.sql.types import StructField,StructType,IntegerType,StringType,LongType

Spark = SparkSession.Builder.appName("Movie_rating").getOrCreate()
schema = StructType([
    StructType("sdgfd",IntegerType(),True),
    StructType("gfdf",IntegerType(),True),
    StructType("Movie_if",StringType(),True)
])

def loadmovie():
    movies={}
    with codecs.open("/home/sai-murali/SparkCourse/","r",encoding="",errors="ignore") as f:
     for file in f:
      field = newdate.split(",")
      movies(field[0] == int(field[1]))
    return movies

newdate = Spark.read.schema(schema).csv("/home/sai-murali/SparkCourse/")
newdat = Spark.broadcast(loadmovie("movie_id"))

def loadmovienames(movie_id):
    return newdat.values(movie_id)

lookupnamesudf = func.udf(loadmovienames)
result = newdat.withcolumn("movienames",lookupnamesudf(func.column("MOviename")))

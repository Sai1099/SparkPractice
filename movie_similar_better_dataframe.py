from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys
import math

def computeJaccardSimilarity(spark, data):
    # Count the number of users who rated both movies
    coRatedPairs = data.groupBy("movie1", "movie2").agg(
        func.count("userID").alias("numPairs")
    )
    
    # Count the number of users who rated each movie individually
    movie1Count = data.groupBy("movie1").agg(func.count("userID").alias("movie1Count"))
    movie2Count = data.groupBy("movie2").agg(func.count("userID").alias("movie2Count"))
    
    # Join the counts with co-rated pairs
    counts = coRatedPairs \
        .join(movie1Count, "movie1") \
        .join(movie2Count, "movie2") \
        .select(
            "movie1", "movie2",
            (func.col("numPairs") / (func.col("movie1Count") + func.col("movie2Count") - func.col("numPairs"))).alias("score")
        )
    
    return counts

def computePearsonSimilarity(spark, data):
    
    movieStats = data.groupBy("movie1", "movie2").agg(
        func.sum("rating1").alias("sum1"),
        func.sum("rating2").alias("sum2"),
        func.sum(func.col("rating1") * func.col("rating1")).alias("sum1sq"),
        func.sum(func.col("rating2") * func.col("rating2")).alias("sum2sq"),
        func.sum(func.col("rating1") * func.col("rating2")).alias("sum12"),
        func.count("rating1").alias("numPairs")
    )
    
  
    pearsonSimilarity = movieStats \
        .withColumn("numerator", func.col("sum12") - (func.col("sum1") * func.col("sum2") / func.col("numPairs"))) \
        .withColumn("denominator", func.sqrt(
            (func.col("sum1sq") - (func.col("sum1") * func.col("sum1") / func.col("numPairs"))) * 
            (func.col("sum2sq") - (func.col("sum2") * func.col("sum2") / func.col("numPairs")))
        )) \
        .withColumn("score", func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")).otherwise(0)) \
        .select("movie1", "movie2", "score", "numPairs")
    
    return pearsonSimilarity

def computeConditionalProbability(spark, data):
    # Count the number of users who rated both movies
    coRatedPairs = data.groupBy("movie1", "movie2").agg(
        func.count("userID").alias("numPairs")
    )
    
    
    movieCount = data.groupBy("movie1").agg(func.count("userID").alias("movieCount"))
    
    
    probabilities = coRatedPairs \
        .join(movieCount, coRatedPairs.movie1 == movieCount.movie1) \
        .select(
            "movie1", "movie2",
            (func.col("numPairs") / func.col("movieCount")).alias("score")
        )
    
    return probabilities

def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]

spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

movieNamesSchema = StructType([ 
    StructField("movieID", IntegerType(), True), 
    StructField("movieTitle", StringType(), True) 
])

moviesSchema = StructType([ 
    StructField("userID", IntegerType(), True), 
    StructField("movieID", IntegerType(), True), 
    StructField("rating", IntegerType(), True), 
    StructField("timestamp", LongType(), True)
])

movieNames = spark.read \
    .option("sep", "|") \
    .option("charset", "ISO-8859-1") \
    .schema(movieNamesSchema) \
    .csv("/home/sai-murali/SparkCourse/ml-100k/u.item")

movies = spark.read \
    .option("sep", "\t") \
    .schema(moviesSchema) \
    .csv("/home/sai-murali/SparkCourse/ml-100k/u.data")

ratings = movies.select("userID", "movieID", "rating")

moviePairs = ratings.alias("ratings1") \
    .join(ratings.alias("ratings2"), (func.col("ratings1.userID") == func.col("ratings2.userID")) \
        & (func.col("ratings1.movieID") < func.col("ratings2.movieID"))) \
    .select(func.col("ratings1.movieID").alias("movie1"), \
            func.col("ratings2.movieID").alias("movie2"), \
            func.col("ratings1.rating").alias("rating1"), \
            func.col("ratings2.rating").alias("rating2"))

# Compute similarities
jaccardSimilarity = computeJaccardSimilarity(spark, moviePairs).cache()
pearsonSimilarity = computePearsonSimilarity(spark, moviePairs).cache()
conditionalProbability = computeConditionalProbability(spark, moviePairs).cache()

if (len(sys.argv) > 1):
    scoreThreshold = 0.5 
    coOccurrenceThreshold = 20  # Minimum number of common raters for Pearson
    
    movieID = int(sys.argv[1])

  
    filteredResultsJaccard = jaccardSimilarity.filter(
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
        (func.col("score") > scoreThreshold)
    ).sort(func.col("score").desc()).take(10)
    
    filteredResultsPearson = pearsonSimilarity.filter(
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
        (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold)
    ).sort(func.col("score").desc()).take(10)
    
    filteredResultsConditional = conditionalProbability.filter(
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
        (func.col("score") > scoreThreshold)
    ).sort(func.col("score").desc()).take(10)
    
    print("Top 10 similar movies for " + getMovieName(movieNames, movieID) + " based on Pearson Correlation")
    for result in filteredResultsPearson:
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
            similarMovieID = result.movie2
        
        print(getMovieName(movieNames, similarMovieID) + "\tscore: " \
              + str(result.score) + "\tstrength: " + str(result.numPairs))
    
    print("\nTop 10 similar movies for " + getMovieName(movieNames, movieID) + " based on Jaccard Similarity")
    for result in filteredResultsJaccard:
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
            similarMovieID = result.movie2
        
        print(getMovieName(movieNames, similarMovieID) + "\tscore: " \
              + str(result.score))
    
    print("\nTop 10 similar movies for " + getMovieName(movieNames, movieID) + " based on Conditional Probability")
    for result in filteredResultsConditional:
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
            similarMovieID = result.movie2
        
        print(getMovieName(movieNames, similarMovieID) + "\tscore: " \
              + str(result.score))

spark.stop()
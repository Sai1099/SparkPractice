from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, window, col, to_timestamp

# Initialize SparkSession
spark = SparkSession.builder.appName("Streaming").getOrCreate()

# Read streaming data from text files
df = spark.readStream.text("/home/sai-murali/SparkCourse/logs")

# Define regular expressions for extracting fields
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

# Extract fields from the log data
logsDF = df.select(
    regexp_extract('value', hostExp, 1).alias('host'),
    regexp_extract('value', timeExp, 1).alias('timestamp'),
    regexp_extract('value', generalExp, 1).alias('method'),
    regexp_extract('value', generalExp, 2).alias('endpoint'),
    regexp_extract('value', generalExp, 3).alias('protocol'),
    regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
    regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size')
)

# Convert 'timestamp' column to TimestampType
logsDF = logsDF.withColumn('timestamp', to_timestamp(col('timestamp'), 'dd/MMM/yyyy:HH:mm:ss Z'))

# Define the window duration and sliding duration
windowedCountsDF = logsDF.groupBy(
    window(col("timestamp"), "30 seconds", "15 seconds"),
    col("status")
).count()

# Define the query to output results to the console
query = (windowedCountsDF.writeStream
    .outputMode("complete")
    .format("console")
    .queryName("status_counts")
    .start())

# Run the query until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()

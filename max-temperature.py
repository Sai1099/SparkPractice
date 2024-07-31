from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster("local").setAppName("Maxtemperature")
sc = SparkContext(conf = conf)

def parselines(line):
    field = line.split(',')
    station_ID = field[0]
    entity_type = field[2]
    temperature = float(field[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (station_ID,entity_type,temperature)







lines = sc.textFile("/home/sai-murali/SparkCourse/1800.csv")
parselines = lines.map(parselines)

maxtemp  = parselines.filter(lambda x: "TMAX" in x[1])
station_id = maxtemp.map(lambda x: (x[0],x[2]))
staoo = station_id.reduceByKey(lambda x,y: max(x,y))

results = staoo.collect();
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
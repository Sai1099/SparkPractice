from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster("local").setAppName('customertotal')
sc = SparkContext(conf = conf)

def parsedline(line):
    row = line.split(',')
    emp_id = int(row[0])
    salary = float(row[2])
    return (emp_id,salary)



lines = sc.textFile('/home/sai-murali/SparkCourse/customer-orders.csv')
parsedline =  lines.map(parsedline)

amount = parsedline.map(lambda x: (x[0],x[1])).reduceByKey(lambda x, y: x + y)



results = amount.collect()

for result in results:
    print(result)
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile('/home/sai-murali/SparkCourse/customer-orders.csv')
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)
sorted = totalByCustomer.map(lambda x: (x[1],x[0])).sortByKey()


results = sorted.collect();
for result in results:
    print(result)

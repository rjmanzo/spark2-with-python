from pyspark import SparkConf, SparkContext
import collections

#parse function for CSV file
def parseLines(line):
    fields = line.split(',')
    Age = int(fields[2])
    totalFriends = int(fields[3])
    return (Age, totalFriends)

# set SparkConf and App name
conf = SparkConf().setMaster("local").setAppName("FriendByAge")
# set SparkContex t with the SparkConf info
sc = SparkContext(conf = conf)

# Read the lines from the archive
lines = sc.textFile("c:/spark2-with-python/datasets/fakefriends.csv")
# parse and create rdd
rdd = lines.map(parseLines)

#map, reduce
# map   --> ex: (33,385) => (33,(385,1))
#       -->     (33,2) => (33,(2,1))
#       -->     (55,221) => (55,(221,1))
# reduce --> ex: (33, (387,2))
totalbyAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y : (x[0] + x[1], y[0] + y[1]))
averagesByAge = totalbyAge.mapValues(lambda x: x[0] / x[1])
# shows results
results = averagesByAge.collect()
for result in results:
    print(result)







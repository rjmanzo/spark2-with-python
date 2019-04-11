from builtins import str, float

from pyspark import SparkConf, SparkContext
import collections

#parse function for CSV file
def parseLines(line):
    fields = line.split(',')
    stationID = str(fields[0])
    entryType = str(fields[2])
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

# set SparkConf and App name
conf = SparkConf().setMaster("local").setAppName("MaxTempbyStation")
# set SparkContex t with the SparkConf info
sc = SparkContext(conf = conf)

# Read the lines from the archive
lines = sc.textFile("c:/spark2-with-python/datasets/1800.csv ")
# parse and create rdd
# Convert to (stationID, entryType, temperature) tuples
rdd = lines.map(parseLines)

# // Filter out all but TMIN entries
minTemps = rdd.filter(lambda x: "TMAX" in x[1])

# // Convert to (stationID, temperature)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))

#  Reduce by stationID retaining the minimum temperature found
minTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))

#flip temps to order stations by temps and order DESC
flipped = minTemps.map(lambda x: (x[1],x[0])).sortByKey(ascending=False)
# // Collect, format, and print the results
results = flipped.collect()

for result in results:
    print(result[1] + "\t{:.2f}F".format(result[0]))
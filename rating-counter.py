from pyspark import SparkConf, SparkContext
import collections

# set SparkConf and App name
conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
# set SparkContext with the SparkConf info
sc = SparkContext(conf = conf)

# Read the lines from the archive
lines = sc.textFile("c:/spark2-with-python/datasets/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))










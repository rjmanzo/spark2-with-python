from builtins import str, float
import re

from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

# set SparkConf and App name
conf = SparkConf().setMaster("local").setAppName("WordCountsBetter")
# set SparkContex t with the SparkConf info
sc = SparkContext(conf = conf)

# Read the lines from the archive
lines = sc.textFile("c:/spark2-with-python/datasets/book.txt")
# Extract word by word using REgular Expressions and normalizing to lowercase
words = lines.flatMap(normalizeWords)

# count the numbers of appereance on each words (the function below create a key/value pair with the word,count)
wordcounts = words.countByValue()

# print the words and the number of appearences
for word,count in wordcounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
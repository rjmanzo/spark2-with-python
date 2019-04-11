from builtins import str, float
import re

from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

# set SparkConf and App name
conf = SparkConf().setMaster("local").setAppName("WordCountsBetterSorted")
# set SparkContex t with the SparkConf info
sc = SparkContext(conf = conf)

# Read the lines from the archive
lines = sc.textFile("c:/spark2-with-python/datasets/book.txt")
# Extract word by word using REgular Expressions and normalizing to lowercase
words = lines.flatMap(normalizeWords)

#commond EnglishDict
CommondWord = ("a", "about", "above", "after", "again", "against", "ain", "all", "am", "an", "and",
     "any", "are", "aren", "aren't", "as", "at", "be", "because", "been", "before", "being", "below",
     "between", "both", "but", "by", "can", "couldn", "couldn't", "d", "did", "didn", "didn't", "do", "does",
     "doesn", "doesn't", "doing", "don", "don't", "down", "during", "each", "few", "for", "from", "further", "had",
     "hadn", "hadn't", "has", "hasn", "hasn't", "have", "haven", "haven't", "having", "he", "her", "here", "hers", "herself",
     "him", "himself", "his", "how", "i", "if", "in", "into", "is", "isn", "isn't", "it", "it's", "its", "itself", "just", "ll",
     "m", "ma", "me", "mightn", "mightn't", "more", "most", "mustn", "mustn't", "my", "myself", "needn", "needn't", "no", "nor",
     "not", "now", "o", "of", "off", "on", "once", "only", "or", "other", "our", "ours", "ourselves", "out", "over", "own", "re",
     "s", "same", "shan", "shan't", "she", "she's", "should", "should've", "shouldn", "shouldn't", "so", "some", "such", "t", "than",
     "that", "that'll", "the", "their", "theirs", "them", "themselves", "then", "there", "these", "they", "this", "those", "through", "to",
     "too", "under", "until", "up", "ve", "very", "was", "wasn", "wasn't", "we", "were", "weren", "weren't", "what", "when", "where", "which",
     "while", "who", "whom", "why", "will", "with", "won", "won't", "wouldn", "wouldn't", "y", "you", "you'd", "you'll", "you're", "you've",
     "your", "yours", "yourself", "yourselves")

# filter commons english words that are useless (using lambda fuction and not filter conditions)
filterWords = words.filter(lambda x: x not in CommondWord)

# counts the appearance of words using map and reduce
wordCounts = filterWords.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# flip key/value and then sort
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)

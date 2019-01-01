from pyspark import SparkConf, SparkContext
sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("local"))

import re

def parse_article(line):
    try:
        article_id, text = unicode(line.rstrip()).split('\t', 1)
        text = re.sub("^\W+|\W+$", "", text, flags=re.UNICODE)
        words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
        return words
    except ValueError as e:
        return []

wiki = sc.textFile("/data/wiki/en_articles_part/articles-part", 16).map(parse_article)

bigrams = wiki.flatMap(lambda x : [(x[i].lower()+"_"+x[i+1].lower()) for i in range(0,len(x)-1)])
word_count = bigrams.filter(lambda x: 'narodnaya_' in x).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)

res = word_count.collect()

for i in res:
    print "%s \t%d" %(i[0],i[1])

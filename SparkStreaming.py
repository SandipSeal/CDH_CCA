from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

import sys

hostname = sys.argv[1]
port = sys.argv[2]

conf = SparkConf(). \
setAppName("Dept Count"). \
setMaster("yarn-client")

sc = SparkContext(conf=conf)
ssc = StreamingContext(sc,30)

messages = ssc.socketTextStream(hostname,int(port))

deptMessages = messages. \
filter (lambda x: x.split(' ')[6].split('/')[1]=="department")

deptName = deptMessages. \
map (lambda x: (x.split(' ')[6].split('/')[2],1))

deptCount = deptName. \
reduceByKey(lambda x, y : x + y)

outputPrefix = sys.argv[3]
deptCount.saveAsTextFiles(outputPrefix)

ssc.start()
ssc.awaitTermination()
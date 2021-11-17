from pyspark import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
# from pyspark.streaming.kafka import KafkaUtils
from time import sleep

import json
sc = SparkContext("local[2]", "Sentiment")
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
ssc = StreamingContext(sc, 1)

sqlcontext=SQLContext(sc)

lines = ssc.socketTextStream('localhost',6100)
words=lines.flatMap(lambda x:x.split("\n"))
words.pprint()

# print(words)
# temp=json.loads(lines)
# print(temp)
ssc.start()
ssc.awaitTermination() 
# ssc.start()
sleep(6)
ssc.stop(stopSparkContext=True, stopGraceFully=True)
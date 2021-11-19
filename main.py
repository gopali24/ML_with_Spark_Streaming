from pyspark import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql import Row, Column
import pyspark.sql.types as tp
# from pyspark.streaming.kafka import KafkaUtils
from time import sleep

import json

def createDataFrame(rdd):
    try:
        print(rdd)
        temp = spark.createDataFrame(rdd)
        # wordsDataFrame=wordsDataFrame.union(temp)
        temp.show()                
    except Exception as e: 
        print(e)

sc = SparkContext("local[2]", "Sentiment")
my_schema = tp.StructType([
    				tp.StructField(name= 'label',       dataType= tp.IntegerType(),  nullable= True),
    				tp.StructField(name= 'tweet',       dataType= tp.StringType(),   nullable= True)    
    			      ])
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
ssc = StreamingContext(sc, 5)
header=['Sentiment','Text']
# emp_RDD=spark.sparkContext.emptyRDD()
# wordsDataFrame = spark.createDataFrame(data=emp_RDD,schema=my_schema)

sqlcontext=SQLContext(sc)

lines = ssc.socketTextStream('localhost',6100)
# words=lines.map(lambda x:)
# print(type(words))
# lines.foreachRDD(lambda x:json.loads(str(x)))
# lines=json.loads(str(lines))
# lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
words = lines.flatMap(lambda line : json.loads(str(line)))
words=words.map(lambda x:x.split(',',1))
words.foreachRDD(createDataFrame)
words.pprint()

# print(words)
# temp=json.loads(lines)
# print(temp)
ssc.start()
ssc.awaitTermination() 
# ssc.start()
sleep(6)
ssc.stop(stopSparkContext=True, stopGraceFully=True)
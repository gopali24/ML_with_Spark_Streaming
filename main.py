from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from functools import reduce
import  pyspark.sql.functions as F
import json
import numpy as np
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import accuracy_score

import warnings
warnings.filterwarnings("ignore")


def Train(X,Y):

    try:
        classifier.partial_fit(X, Y , classes=list(range(2)))

    except Exception as e: 
        print("error in traning:",e)

    try:
        Y_test_preds = []
        for k in range(1,test_x.shape[0]): ## Looping through test batches for making predictions
            Y_preds = classifier.predict(test_x[k])
            Y_test_preds.extend(Y_preds.tolist())

        print("Test Accuracy      : ",accuracy_score(test_y.reshape(-1), Y_test_preds))

    except Exception as e: 
        print("error in accuracy:",e)
    

def createDataFrame(rdd):
    try:
        # creating the partial dataframe
        temp = spark.createDataFrame(rdd)
        oldColumns = temp.schema.names
        newColumns = ["Label", "Tweet"]
        temp= reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), temp)
        

        #doing preprocessing
        df_clean=temp.dropna(subset=['Tweet'])
        df_select_clean = (df_clean.withColumn("Tweet", F.regexp_replace("Tweet", r"[@#&][A-Za-z0-9-]+", " "))
                       .withColumn("Tweet", F.regexp_replace("Tweet", r"\w+://\S+", " "))
                       .withColumn("Tweet", F.regexp_replace("Tweet", r"[^A-Za-z]", " "))
                       .withColumn("Tweet", F.regexp_replace("Tweet", r"\s+", " "))
                       .withColumn("Tweet", F.lower(F.col("Tweet")))
                       .withColumn("Tweet", F.trim(F.col("Tweet")))
                      ) 

        # printing the final DataFrame
        df_select_clean.show(truncate=False)
        
        
        
        train_y = test_df.select(F.collect_list('Label')).first()[0]
        train_x = test_df.select(F.collect_list('Tweet')).first()[0]
        Train(train_x,train_y)


    except Exception as e: 
        print(e)



# getting the spark context
sc = SparkContext("local[2]", "Sentiment")
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
ssc = StreamingContext(sc, 5)

# making sore
sc.setLogLevel("ERROR")
spark.sparkContext.setLogLevel("ERROR")


# creating a classifier
classifier = SGDClassifier()



# getting the test dataset
test_df = spark.read.csv("my_test.csv")
oldColumns = test_df.schema.names
newColumns = ["Label", "Tweet"]
test_df= reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), test_df)
test_df.show()
test_y = test_df.select(F.collect_list('Label')).first()[0]
test_x = test_df.select(F.collect_list('Tweet')).first()[0]




# getting the streaming contents
lines = ssc.socketTextStream('localhost',6100)
words = lines.flatMap(lambda line : json.loads(str(line)))
words=words.map(lambda x:x.split(',',1))

# function to create a data frame from the streamed data
words.foreachRDD(createDataFrame)


# starting the stream
ssc.start()
ssc.awaitTermination()
ssc.stop(stopSparkContext=True, stopGraceFully=True)



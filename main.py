from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from functools import reduce
import  pyspark.sql.functions as F
import json,math
import numpy as np
from sklearn.linear_model import SGDClassifier,SGDRegressor
from sklearn.metrics import accuracy_score
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import confusion_matrix
from sklearn.metrics import f1_score
from sklearn.metrics import mean_squared_error




import warnings
warnings.filterwarnings("ignore")



def vectrize(x_test):
    cv = CountVectorizer(max_features = 2500)
    x = cv.fit_transform(x_test).toarray()
    sc = StandardScaler()
    x = sc.fit_transform(x)
    return x


def Train(X,Y):
    cv = CountVectorizer(max_features = 2500)
    x = cv.fit_transform(X).toarray()
    y=Y
    sc = StandardScaler()
    x = sc.fit_transform(x)

    # print("traning:",x,y)


    try:
        # classifier.partial_fit(X, Y , classes=list(range(2)))
        classifier.partial_fit(x, y)

    except Exception as e: 
        print("error in traning:",e)

    try:

        
        y_pred = classifier.predict(test_x)
        y_pred=list(map(math.ceil,y_pred))
        print(test_y,y_pred)
        print("Training Accuracy :", classifier.score(x, y))
        print("Test Accuracy :", classifier.score(test_x, test_y))
        print("F1 score :", f1_score(test_y, y_pred))
        cm = confusion_matrix(test_y, y_pred)
        print(cm)
        print("RMSE:",mean_squared_error(test_y, y_pred))

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
        train_y=list(map(int,train_y[1:]))
        train_y=[1 if x==4 else x for x in train_y]
        train_x = test_df.select(F.collect_list('Tweet')).first()[0][1:]

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
# classifier = SGDClassifier()
classifier = SGDRegressor()


# getting the test dataset
test_df = spark.read.csv("test.csv")
oldColumns = test_df.schema.names
newColumns = ["Label", "Tweet"]
test_df= reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), test_df)
# test_df.show(truncate=False)
test_y = test_df.select(F.collect_list('Label')).first()[0]
test_y=list(map(int,test_y[1:]))
test_y=[1 if x==4 else x for x in test_y]
test_x = test_df.select(F.collect_list('Tweet')).first()[0][1:]
test_x=vectrize(test_x)




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



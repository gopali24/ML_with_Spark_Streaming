from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from functools import reduce
import  pyspark.sql.functions as F
import json
import numpy as np
from pyspark.ml.feature import Tokenizer, StopWordsRemover,IDF
from pyspark.sql.types import *
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import accuracy_score
from nltk.stem.snowball import SnowballStemmer
from pyspark.ml import Pipeline
from pyspark.ml.clustering import LDA
import warnings
warnings.filterwarnings("ignore")


# def Train(X,Y):

    # try:
    # 	tfidfvec = TfidfVectorizer(stop_words = "english",analyzer = 'word',lowercase = True,use_idf = True,ngram_range = (2,2))
    # 	X_train = tfidfvec.fit_transform(X)
    # 	classifier.partial_fit(X, Y , classes=list(range(2)))

    # except Exception as e: 
    #     print("error in traning:",e)

    # try:
    #     Y_test_preds = []
    #     for k in range(1,test_x.shape[0]): ## Looping through test batches for making predictions
    #         Y_preds = classifier.predict(test_x[k])
    #         Y_test_preds.extend(Y_preds.tolist())

    #     print("Test Accuracy      : ",accuracy_score(test_y.reshape(-1), Y_test_preds))

    # except Exception as e: 
    #     print("error in accuracy:",e)
    

def createDataFrame(rdd):
    try:
        # creating the partial dataframe
        temp = spark.createDataFrame(rdd)
        oldColumns = temp.schema.names
        newColumns = ["Label", "Tweet"]
        temp= reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), temp)
        

        #doing preprocessing
        df_clean=temp.dropna(subset=['Tweet'])
        df_clean=df_clean.select("*").withColumn("id",F.monotonically_increasing_id())
        df_select_clean = (df_clean.withColumn("Tweet", F.regexp_replace("Tweet", r"[@#&][A-Za-z0-9-]+", " "))
                       .withColumn("Tweet", F.regexp_replace("Tweet", r"\w+://\S+", " "))
                       .withColumn("Tweet", F.regexp_replace("Tweet", r"[^A-Za-z]", " "))
                       .withColumn("Tweet", F.regexp_replace("Tweet", r"\s+", " "))
                       .withColumn("Tweet", F.lower(F.col("Tweet")))
                       .withColumn("Tweet", F.trim(F.col("Tweet")))
                      ) 
        # Tokenize text
        tokenizer = Tokenizer(inputCol='Tweet', outputCol='words_token')
        df_words_token = tokenizer.transform(df_select_clean).select('id', 'words_token')

        # Remove stop words
        remover = StopWordsRemover(inputCol='words_token', outputCol='words_clean')
        df_words_no_stopw = remover.transform(df_words_token).select('id', 'words_clean')

        # Stem text
        stemmer = SnowballStemmer(language='english')
        stemmer_udf = F.udf(lambda tokens: [stemmer.stem(token) for token in tokens], ArrayType(StringType()))
        df_stemmed = df_words_no_stopw.withColumn("words_stemmed", stemmer_udf("words_clean")).select('id', 'words_stemmed')

        # printing the final DataFrame
        df_stemmed.show(truncate=False)
        vectorizer = CountVectorizer(inputCol= "words_stemmed", outputCol="rawFeatures")
        # 2.5. IDf
        idf = IDF(inputCol="rawFeatures", outputCol="features")

        # 3. LDA model
        lda = LDA(k=2,optimizer="em")

        pipeline = Pipeline(stages=[vectorizer, idf, lda])

        pipeline_model = pipeline.fit(df_stemmed)
        pipeline_model.write().overwrite().save('mymodel.pkl')
        
        
        
        #train_y = df_select_clean.select(F.collect_list('Label')).first()[0]
        #train_x = df_select_clean.select(F.collect_list('Tweet')).first()[0]
        # Train(train_x,train_y)


    except Exception as e: 
        print(e)
        # ssc.stop(stopSparkContext=True, stopGraceFully=True)



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
'''
test_df = spark.read.csv("my_test.csv")
oldColumns = test_df.schema.names
newColumns = ["Label", "Tweet"]
test_df= reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), test_df)
test_df.show()
test_y = test_df.select(F.collect_list('Label')).first()[0]
test_x = test_df.select(F.collect_list('Tweet')).first()[0]
'''



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



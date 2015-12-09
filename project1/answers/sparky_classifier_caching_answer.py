#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.sql import SQLContext
#Import relevant libraries
from pyspark.storagelevel import StorageLevel

from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer

import re
import sys
import numpy as np
import time

#Paths to train data and labels
PATH_TO_JSON = "/user/alexeys/BigDataCourse/web_dataset_preprocessed/part-00000"
PATH_TO_TRAIN_LABELS = "/user/alexeys/BigDataCourse/web_dataset_labels/train.json"

# Module-level global variables for the `tokenize` function below
STOPWORDS = stopwords.words('english')
STEMMER = SnowballStemmer("english", ignore_stopwords=True)

# Function to break text into "tokens"
def tokenize(text):
    tokens = word_tokenize(text)
    no_stopwords = filter(lambda x: x not in STOPWORDS,tokens)
    stemmed = map(lambda w: STEMMER.stem(w),no_stopwords)
    s = set(stemmed)
    stemmed = list(s)
    return filter(None,stemmed)

def main(argv):
    start = time.time()
    
    #PARSE DATA INTO DATA FRAME OR TEMP. TABLE
    print "Parse data..."
    sc = SparkContext(appName="Classification")
    sqlContext = SQLContext(sc)
    input_schema_rdd = sqlContext.read.json(PATH_TO_JSON)
    train_label_rdd = sqlContext.read.json(PATH_TO_TRAIN_LABELS)

    # SQL can be run over DataFrames that have been registered as a table.
    input_schema_rdd.registerTempTable("input")
    train_label_rdd.registerTempTable("train_label")
    
    #Make RDD with labels
    train_wlabels_0 = sqlContext.sql("SELECT title,text,images,links,label FROM input JOIN train_label WHERE input.id = train_label.id AND label = 0")
    train_wlabels_1 = sqlContext.sql("SELECT title,text,images,links,label FROM input JOIN train_label WHERE input.id = train_label.id AND label = 1")

    #FEATURE ENGINEERING
    print "Feature extraction..."
    #First, get text features only
    text_only_0 = train_wlabels_0.map(lambda p: p.text)
    text_only_1 = train_wlabels_1.map(lambda p: p.text)
    #print text_only_0.take(3)

    #Extract word frequencies in the corpus
    #numFeatures is a free parameter
    tf = HashingTF(numFeatures=10000)
    tokenized_0 = text_only_0.map(lambda line: tokenize(line))
    count_vectorized_0 = tf.transform(tokenized_0).cache()
    tokenized_1 = text_only_1.map(lambda line: tokenize(line))
    count_vectorized_1 = tf.transform(tokenized_1).cache()

    #calculating IDF 
    idf_0 = IDF(minDocFreq=2).fit(count_vectorized_0)
    tfidf_0 = idf_0.transform(count_vectorized_0)
    idf_1 = IDF(minDocFreq=2).fit(count_vectorized_1)
    tfidf_1 = idf_1.transform(count_vectorized_1)
    tfidf_0.count()
    tfidf_1.count()

    endtime = time.time()
    print "Elapsed time: ", endtime-start

if __name__ == "__main__":
   main(sys.argv)

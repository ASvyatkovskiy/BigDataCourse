#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.storagelevel import StorageLevel

#Import necessary classifiers
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, ...
from pyspark.mllib.tree ...

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

# Load and parse the data in the format good for classification
def parsePoint(label,feature):
    return LabeledPoint(label, feature.toArray())

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

    #convert into a format expected by MLlib classifiers
    #Which is a Labeled point
    labeled_tfidf_0 = tfidf_0.map(...)
    labeled_tfidf_1 = tfidf_1.map(...)
    labeled_tfidf = labeled_tfidf_0.union(labeled_tfidf_1)

    #CV, MODEL SELECTION, AND CLASSIFICATION STEP
    print "Classification..."
    (trainData, cvData) = labeled_tfidf.randomSplit([0.7, 0.3])
    trainData.cache()
    cvData.cache()

    #Try various classifiers
    #Logistic regression
    model = LogisticRegressionWithLBFGS.train(trainData,iterations=10,regParam=0.01,regType="l1",numClasses=2)

    #Try training an SVM model instead
    #model = 

    #Try training random forest classifier
    #model = RandomForest. ...

    #EVALUATION STEP
    # Evaluate model on test instances and compute test error
    predictions = model.predict(cvData.map(lambda x: x.features))
    labelsAndPreds = cvData.map(lambda lp: lp.label).zip(predictions)
    trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(cvData.count())
    print('CV Error = ' + str(trainErr))

    endtime = time.time()
    print "Elapsed time: ", endtime-start

if __name__ == "__main__":
   main(sys.argv)

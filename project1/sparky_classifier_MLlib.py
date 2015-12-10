#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql import SQLContext

from pyspark.mllib.classification import LogisticRegressionWithLBFGS,SVMWithSGD,NaiveBayes
from pyspark.mllib.tree import RandomForest
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

# put in the path to the kaggle data
PATH_TO_JSON = "/user/alexeys/KaggleDato/Preprocessed_new/"
PATH_TO_TRAIN_LABELS = "/scratch/network/alexeys/KaggleDato/train_v2.json"

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

# Load and parse the data
def parsePoint(label,beast):
    #arraypart = beast.toArray()
    #adhoc non-text features
    #links = beast[0][1]
    #adhocpart = np.array([image,links,title])
    #LabeledPoint(label, np.hstack((arraypart,adhocpart)))

    return LabeledPoint(label, beast.toArray())

def clean_text(text_as_string):
    text_as_string = text_as_string.encode("utf8").translate(None,'=@&$/%?<>,[]{}()*.0123456789:;-\n\'"_').lower()
    text_as_string = re.sub(' +',' ',text_as_string)
    return text_as_string

def main(argv):
    start = time.time()
    
    #DATA PARSING STEP
    print "Parse data..."
    sc = SparkContext(appName="Test_Step2")
    sqlContext = SQLContext(sc)

    input_schema_rdd = sqlContext.read.json("file:///scratch/network/alexeys/KaggleDato/Preprocessed_new/0_1/part-*")
    #input_schema_rdd.show() 
    #input_schema_rdd.printSchema()
    #input_schema_rdd.select("id").show()

    train_label_rdd = sqlContext.read.json("file://"+PATH_TO_TRAIN_LABELS)

    #df = person_df.join(relationships_df, person_df.id == relationships_df.person_id)
    #combined_df = df.join(movie_df,movie_df.id == df.movie_id)
    #answer = combined_df.where(combined_df.name == "Keanu Reeves")
    ##input_df.select('born', 'name').write.format('com.databricks.spark.csv').save('/user/alexeys/BigDataCourse/csv/newcars.csv')
    #print answer.select('name','title','roles').show()

    # SQL can be run over DataFrames that have been registered as a table.
    input_schema_rdd.registerTempTable("input")
    train_label_rdd.registerTempTable("train_label")
    train_wlabels_0 = sqlContext.sql("SELECT title,text,images,links,label FROM input JOIN train_label WHERE input.id = train_label.id AND label = 0")
    train_wlabels_1 = sqlContext.sql("SELECT title,text,images,links,label FROM input JOIN train_label WHERE input.id = train_label.id AND label = 1")

    #list of unicode strings, one string - 1 document 
    text_only_0 = train_wlabels_0.map(lambda p: p.text) #getLists(p.text)).map(lambda p: clean_text(p))
    text_only_1 = train_wlabels_1.map(lambda p: p.text) #getLists(p.text)).map(lambda p: clean_text(p))
 
    #get custom features
    #image_only_0 = train_wlabels_0.map(lambda p: p.images)
    #image_only_1 = train_wlabels_1.map(lambda p: p.images)
    #links_only_0 = train_wlabels_0.map(lambda p: p.links)
    #links_only_1 = train_wlabels_1.map(lambda p: p.links)

    #FEATURE ENGINEERING
    print "Feature extraction..."
    #count text frequencies
    tf = HashingTF(numFeatures=10000) #can do 8k
    tokenized_0 = text_only_0.map(lambda line: tokenize(line))
    count_vectorized_0 = tf.transform(tokenized_0).cache()
    #count_vectorized_0 has a list of SparseVectors
    #[SparseVector(10, {0: 33.0, 1: 53.0, 2: 38.0, 3: 33.0, 4: 38.0, 5: 44.0, 6: 51.0, 7: 38.0, 8: 46.0, 9: 45.0}), SparseVector(10, {0: 6.0, 1: 5.0, 2: 5.0, 3: 4.0, 4: 6.0, 5: 8.0, 6: 7.0, 7: 3.0, 8: 8.0, 9: 4.0}), SparseVector(10, {0: 44.0, 1: 28.0, 2: 32.0, 3: 33.0, 4: 28.0, 5: 44.0, 6: 32.0, 7: 32.0, 8: 42.0, 9: 33.0})]

    tokenized_1 = text_only_1.map(lambda line: tokenize(line))
    count_vectorized_1 = tf.transform(tokenized_1).cache()

    #add them adhoc non-text features
    #documents_0 = text_documents_0.zip(image_only_0).zip(links_only_0)
    #documents_1 = text_documents_1.zip(image_only_1).zip(links_only_1)

    #calculating IDF 
    idf_0 = IDF(minDocFreq=2).fit(count_vectorized_0)
    tfidf_0 = idf_0.transform(count_vectorized_0)
    idf_1 = IDF(minDocFreq=2).fit(count_vectorized_1)
    tfidf_1 = idf_1.transform(count_vectorized_1)

    #convert into a format expected by MLlib classifiers
    labeled_tfidf_0 = tfidf_0.map(lambda row: parsePoint(0,row))
    labeled_tfidf_1 = tfidf_1.map(lambda row: parsePoint(1,row))
    labeled_tfidf = labeled_tfidf_0.union(labeled_tfidf_1).cache()
    #[SparseVector(10, {0: 5.087, 1: 8.17, 2: 5.8577, 3: 11.1036, 4: 5.8577, 5: 6.7826, 6: 7.8617, 7: 9.1642, 8: 7.0909, 9: 6.9368}), SparseVector(10, {0: 0.9249, 1: 0.7708, 2: 0.7708, 3: 1.3459, 4: 0.9249, 5: 1.2332, 6: 1.0791, 7: 0.7235, 8: 1.2332, 9: 0.6166}), SparseVector(10, {0: 6.7826, 1: 4.3162, 2: 4.9328, 3: 11.1036, 4: 4.3162, 5: 6.7826, 6: 4.9328, 7: 7.7172, 8: 6.4743, 9: 5.087})]

    #CV, MODEL SELECTION, AND CLASSIFICATION STEP
    print "Classification..."
    (trainData, cvData) = labeled_tfidf.randomSplit([0.7, 0.3])
    trainData.cache()
    cvData.cache()
    #print cvData.take(10)

    #Try various classifiers
    #With logistic regression only use training data
    #weights = [0.0]*104
    #map(lambda x: x+100,weights[103:])
    #map(lambda x: x+0.5,weights[:10])
    #model = LogisticRegressionWithLBFGS.train(trainData,iterations=10,initialWeights=weights,regParam=0.01,regType="l2",numClasses=2)
    #model = LogisticRegressionWithLBFGS.train(trainData,iterations=10,initialWeights=weights,regParam=0.01,regType="l1",numClasses=2)

    #data, iterations=100, initialWeights=None, regParam=0.01, regType="l2",
    #          intercept=False, corrections=10, tolerance=1e-4, validateData=True, numClasses=2):
    #SVM
    #model = SVMWithSGD.train(trainData,iterations=200,regParam=10) 
    #data, iterations=100, step=1.0, regParam=0.01,
    #          miniBatchFraction=1.0, initialWeights=None, regType="l2",
    #          intercept=False, validateData=True):
    #Logistic regression works a lot better
    #model = NaiveBayes.train(trainData)
    #random forest
    model = RandomForest.trainClassifier(trainData, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=200, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth=4, maxBins=32)

    #EVALUATION STEP
    # Evaluate model on test instances and compute test error
    predictions = model.predict(cvData.map(lambda x: x.features))
    labelsAndPreds = cvData.map(lambda lp: lp.label).zip(predictions)
    trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(cvData.count())
    print('CV Error = ' + str(trainErr))
    #print('Learned classification forest model:')
    #print(model.toDebugString())

    endtime = time.time()
    print "Elapsed time: ", endtime-start

if __name__ == "__main__":
   main(sys.argv)

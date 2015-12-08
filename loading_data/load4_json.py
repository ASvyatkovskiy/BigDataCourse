from pyspark import SparkContext
from pyspark.sql import SQLContext

import json
import sys

def main_sqlcontext(args):
    sc = SparkContext(appName="LoadJson")
    sqlContext = SQLContext(sc)

    input = sqlContext.read.json("/user/alexeys/BigDataCourse/json/")
    input.printSchema()
    input.registerTempTable("movies")
    answer = sqlContext.sql("SELECT * FROM movies WHERE title LIKE '%Atlas%'")
    answer.show()

def main_unstructured(args):
    sc = SparkContext(appName="LoadJson")

    input = sc.textFile("/user/alexeys/BigDataCourse/json/")
    data = input.map(lambda x: json.loads(x)).filter(lambda x: 'Atlas' in x['title'])
    answer = data.collect() 
    print answer[0]['name']," ",answer[0]['roles']," ",answer[0]['title']

if __name__ == "__main__":
    main_sqlcontext(sys.argv)
    #main_unstructured(sys.argv)

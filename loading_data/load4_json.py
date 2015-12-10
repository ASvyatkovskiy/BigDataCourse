from pyspark import SparkContext
from pyspark.sql import SQLContext

import json
import sys
import os

def main_sqlcontext(args):
    sc = SparkContext(appName="LoadJson")
    sqlContext = SQLContext(sc)

    input = sqlContext.read.json(os.environ.get('SCRATCH_PATH')+"/BigDataCourse/json/")
    input.printSchema()
    input.registerTempTable("movies")
    answer = sqlContext.sql("SELECT * FROM movies WHERE title LIKE '%Atlas%'")
    answer.repartition(1).write.save(os.environ.get('SCRATCH_PATH')+"/output_json1/", format="json")

def main_unstructured(args):
    sc = SparkContext(appName="LoadJson")

    input = sc.textFile(os.environ.get('SCRATCH_PATH')+"/BigDataCourse/json/")
    data = input.map(lambda x: json.loads(x)).filter(lambda x: 'Atlas' in x['title'])
    data.repartition(1).saveAsTextFile(os.environ.get('SCRATCH_PATH')+"output_json2")

if __name__ == "__main__":
    main_sqlcontext(sys.argv)
    #main_unstructured(sys.argv)

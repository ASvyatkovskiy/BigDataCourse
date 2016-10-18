from pyspark.sql import SparkSession

import json
import sys
import os

def main_sqlcontext(args):
    spark = SparkSession.builder.appName("LoadJson").getOrCreate()

    input = spark.read.json(os.environ.get('SCRATCH_PATH')+"/json/")
    input.printSchema()
    input.registerTempTable("movies")
    answer = spark.sql("SELECT * FROM movies WHERE title LIKE '%Atlas%'")
    answer.coalesce(1).write.save(os.environ.get('SCRATCH_PATH')+"/output_json1/", format="json")

def main_unstructured(args):
    spark = SparkSession.builder.appName("LoadJson").getOrCreate()

    input = spark.sparkContext.textFile(os.environ.get('SCRATCH_PATH')+"/json/")
    data = input.map(lambda x: json.loads(x)).filter(lambda x: 'Atlas' in x['title'])
    data.coalesce(1).saveAsTextFile(os.environ.get('SCRATCH_PATH')+"/output_json2")

if __name__ == "__main__":
    main_sqlcontext(sys.argv)
    #main_unstructured(sys.argv)

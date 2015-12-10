from pyspark import SparkContext
from pyspark.sql import SQLContext
import csv
import sys
import StringIO
import os

#this one is use when you use textFile
def loadRecord(line,header,delimiter):
    """Parse a CSV line"""
    input = StringIO.StringIO(line)
    reader = csv.DictReader(input, delimiter=delimiter, fieldnames=header)
    return reader.next()

def main_rdd(args):
    sc = SparkContext(appName="LoadCsv")
    delimiter = "|"

    # Try the record-per-line-input
    input = sc.textFile("/scratch/network/alexeys/BigDataCourse/csv/person_nodes2.csv")
    header = input.first().split(delimiter)
    data = input.filter(lambda x: header[0] not in x).map(lambda x: loadRecord(x,header,delimiter))
    data.repartition(1).saveAsTextFile(os.environ.get('SCRATCH_PATH')+"/output_csv1/")

def main_dataframe(args):
    sc = SparkContext(appName="LoadCsv")

    delimiter = "|"

    #csv into spark dataframe   
    #this requires using the databricks/spark-csv
    #spark-submit --packages com.databricks:spark-csv_2.10:1.3.0
    sqlContext = SQLContext(sc)

    input_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true',delimiter=delimiter).load('/scratch/network/alexeys/BigDataCourse/csv/person_nodes.csv')
    input_df.write.format("com.databricks.spark.csv").option("header", "true").save(os.environ.get('SCRATCH_PATH')+"/output_csv2/")

 
if __name__ == "__main__":
    #Load into a regular RDD using textFile and parsing the CSV file line by line
    main_rdd(sys.argv)
    
    #Load into dataframe using the csv reader from Databricks
    #main_dataframe(sys.argv)

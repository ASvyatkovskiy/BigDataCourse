from pyspark import SparkContext
from pyspark.sql import SQLContext
import csv
import sys
import StringIO

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
    input = sc.textFile("/user/alexeys/BigDataCourse/csv/person_nodes.csv")
    header = input.first().split(delimiter)
    data = input.filter(lambda x: header[0] not in x).map(lambda x: loadRecord(x,header,delimiter))
    print data.take(5)

def main_dataframe():
    sc = SparkContext(appName="LoadCsv")
    delimiter = "|"

    #csv into spark dataframe   
    #this requires using the databricks/spark-csv
    #spark-submit argument --packages com.databricks:spark-csv_2.10:1.3.0
    sqlContext = SQLContext(sc)

    input_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true',delimiter=delimiter).load('/user/alexeys/BigDataCourse/csv/person_nodes.csv')
    print input_df.take(5)
 
if __name__ == "__main__":
    #Load into a regular RDD using textFile and parsing the CSV file line by line
    main_rdd(sys.argv)
    
    #Load into dataframe using the csv reader from Databricks
    #main_dataframe(sys.argv)

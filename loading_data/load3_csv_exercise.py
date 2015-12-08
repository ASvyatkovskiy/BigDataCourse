from pyspark import SparkContext
from pyspark.sql import SQLContext

import sys
import time

#Exercise: use what you have learned in the load2_csv.py exercise to load a set of CSV datasets 
#and find movies where Tom Hanks played in

#run
# spark-submit --master local[*] --packages com.databricks:spark-csv_2.10:1.3.0 load3_csv_answer.py


def main(args):
    start  = time.time()
 
    sc = SparkContext(appName="LoadCsv")
    delimiter = "|"

    #Load 3 csv files into spark dataframe   
    #this requires using the databricks/spark-csv
    #spark-submit argument --packages com.databricks:spark-csv_2.10:1.3.0
    sqlContext = SQLContext(sc)
    person_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true',delimiter=delimiter).load('/user/alexeys/BigDataCourse/csv/person_nodes.csv')
    movie_df = 
    relationships_df = 

    #Prepare a linked dataset of people, movies and the roles for people who played in those movies
    df = person_df.join(...
    combined_df = df.join(...

    #Use where statement analogous to that in Pandas dataframes to find movies associated with name "Tom Hanks"
    answer = combined_df.where(...

    #Return only actor name, movie title and roles
    print answer.select(...).show()

    #Save the answer in JSON format 
    answer.select(...).write.save("/user/alexeys/BigDataCourse/json/", format="json")

    end = time.time()
    print "Elapsed time: ", (end-start)


if __name__ == "__main__":
    main(sys.argv)

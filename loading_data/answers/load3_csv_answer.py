from pyspark import SparkContext
from pyspark.sql import SQLContext

import sys
import time
import os

#Exercise: use what you have learned in the load2_csv.py exercise to load a set of CSV datasets 
#and find movies where Tom Hanks played in
def main(args):
    start  = time.time()
 
    sc = SparkContext(appName="LoadCsv")
    delimiter = "|"

    #Load 3 csv files into spark dataframe   
    sqlContext = SQLContext(sc)
    person_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true',delimiter=delimiter).load('/scratch/network/alexeys/BigDataCourse/csv/person_nodes.csv')
    movie_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true',delimiter=delimiter).load('/scratch/network/alexeys/BigDataCourse/csv/movie_nodes.csv')
    relationships_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true',delimiter=delimiter).load('/scratch/network/alexeys/BigDataCourse/csv/acted_in_rels.csv')

    #Prepare a linked dataset of people, movies and the roles for people who played in those movies
    df = person_df.join(relationships_df, person_df.id == relationships_df.person_id) 
    combined_df = df.join(movie_df,movie_df.id == df.movie_id)

    #Use where statement analogous to that in Pandas dataframes to find movies associated with name "Tom Hanks"
    answer = combined_df.where(combined_df.name == "Tom Hanks")

    #Return only actor name, movie title and roles
    print answer.select('name','title','roles').show()

    #Save the answer in JSON format 
    answer.repartition(1).select('name','title','roles').write.save(os.environ.get('SCRATCH_PATH')+"/BigDataCourse/json/", format="json")

    end = time.time()
    print "Elapsed time: ", (end-start)


if __name__ == "__main__":
    main(sys.argv)

from pyspark import SparkContext

import sys
import time
import re
import os
from operator import add

def main(args,npartitions):
    sc = SparkContext(appName="LoadJson")

    start = time.time()
    lines = sc.textFile(os.environ.get('SCRATCH_PATH')+"/BigDataCourse/large/", npartitions)
    print "Number of elements in input dataframe: ", lines.count()

    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    #Uncomment for exercise 2
    #counts.cache()

    counts.saveAsTextFile(os.environ.get('SCRATCH_PATH')+"/BigDataCourse/output_large1/")

    #Uncomment for exercise 2
    #counts2 = lines.flatMap(lambda x: x.split(' ')) \
    #              .map(lambda x: (x, 1)) \
    #              .reduceByKey(add)
    #counts2.saveAsTextFile(os.environ.get('SCRATCH_PATH')+"/BigDataCourse/output_large2/")

    end = time.time()
    print "Elapsed time: ", end-start
    sc.stop()

if __name__ == "__main__":
    #Exercise 1: try running with 10 partitions, time it, then change to default 2 partitions
    #Exercise 2: repeat the word count twice (by uncommenting the section in the main() and caching the input dataset,
    #then repeat the same without caching and compare times 
    npartitions = 10
    main(sys.argv,npartitions)

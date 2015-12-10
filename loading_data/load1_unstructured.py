from pyspark import SparkContext
import sys
import time
import os

def main1(args):
    start = time.time()
    sc = SparkContext(appName="LoadUnstructured")

    #By default it assumes file located on hdfs folder, 
    #but by prefixing "file://" it will search the local file system
    #Can specify a folder, can pass list of folders or use wild character
    input_rdd = sc.textFile("/scratch/network/alexeys/BigDataCourse/unstructured/")

    counts = input_rdd.flatMap(lambda line: line.split()) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

    print "\nTaking the 10 most frequent words in the text and corresponding frequencies:"
    #print counts.takeOrdered(10, key=lambda x: -x[1])

    counts.map(lambda (a, b): (b, a)).sortByKey(0).map(lambda (a, b): (b, a)).repartition(1).saveAsTextFile("file://"+os.environ.get('SCRATCH_PATH')+"/output_loadunstructured1/")

    end = time.time()
    print "Elapsed time: ", (end-start)

def main2(args):
    start = time.time()
    sc = SparkContext(appName="LoadUnstructured")

    #Use alternative approach: load the dinitial file into a pair RDD
    input_pair_rdd = sc.wholeTextFiles("/scratch/network/alexeys/BigDataCourse/unstructured/")

    counts = input_pair_rdd.flatMap(lambda line: line[1].split()) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

    print "\nTaking the 10 most frequent words in the text and corresponding frequencies:"
    #print counts.takeOrdered(10, key=lambda x: -x[1])
    counts.map(lambda (a, b): (b, a)).sortByKey(0).map(lambda (a, b): (b, a)).repartition(1).saveAsTextFile("file://"+os.environ.get('SCRATCH_PATH')+"/output_loadunstructured2/")

    end = time.time()
    print "Elapsed time: ", (end-start)
    sc.stop()

if __name__ == "__main__":
    #We are going to run these exercises using the Slurm submission scripts again

    # Try the record-per-line-input
    main1(sys.argv)

    #Use alternative approach: load the initial file into a pair RDD
    #main2(sys.argv)

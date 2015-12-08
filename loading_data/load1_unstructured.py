from pyspark import SparkContext
import sys
import time

def main1(args):
    start = time.time()
    sc = SparkContext(appName="LoadUnstructured")

    #By default it assumes file located on hdfs folder, 
    #but by prefixing "file://" it will search the local file system
    #Can specify a folder, can pass list of folders or use wild character
    input_rdd = sc.textFile("/user/alexeys/BigDataCourse/unstructured/")

    #inspect it, understand how it is structured (list of strings-lines)
    print input_rdd.take(10)
    print "Input dataset has ", input_rdd.count(), " lines"

    counts = input_rdd.flatMap(lambda line: line.split()) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

    print "\nTaking the 10 most frequent words in the text and corresponding frequencies:"
    print counts.takeOrdered(10, key=lambda x: -x[1])
    end = time.time()
    print "Elapsed time: ", (end-start)

def main2(args):
    start = time.time()
    sc = SparkContext(appName="LoadUnstructured")

    #Use alternative approach: load the dinitial file into a pair RDD
    input_pair_rdd = sc.wholeTextFiles("/user/alexeys/BigDataCourse/unstructured/")

    #inspect it, understand how it is structured (list of strings-lines)
    print input_pair_rdd.take(3)
    print "Input dataset has ", input_pair_rdd.count(), " files"
    counts = input_pair_rdd.flatMap(lambda line: line[1].split()) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
    print "\nTaking the 10 most frequent words in the text and corresponding frequencies:"
    print counts.takeOrdered(10, key=lambda x: -x[1])
    end = time.time()
    print "Elapsed time: ", (end-start)


if __name__ == "__main__":
    # Try the record-per-line-input
    main1(sys.argv)
    #Use alternative approach: load the initial file into a pair RDD
    #main2(sys.argv)

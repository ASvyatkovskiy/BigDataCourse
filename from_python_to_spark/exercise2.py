#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.storagelevel import StorageLevel

import sys

def basic():
    numbers = [1,2,3,4,5]
    squares = []
    for number in numbers:
        if number < 4:
            squares.append(number*number)
    print "Result of the local calculation: ", squares

def basic_pythonic():
    numbers = [1,2,3,4,5]
    squares = map(lambda x: x*x, filter(lambda x: x < 4, numbers))
    print "Result of the local pythonic calculation: ", squares

def main(args):
    #Exercise4: from Python to PySpark
    basic()

    #Before re-writing it in PySpark, re-write it using map and filter expressions
    basic_pythonic()

    #Now do with PySpark
    sc = SparkContext("My First App")
    numbers_rdd = sc.parallelize(numbers)
    squares_rdd = numbers_rdd.filter(lambda x: x < 4).map(lambda x: x*x)
    print "Result of the distributed calculation with PySpark: ", squares_rdd.collect()

if __name__='__main__':
    main(sys.argv)

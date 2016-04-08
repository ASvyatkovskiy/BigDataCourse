#!/usr/bin/env python

#Uncomment the following import line for the exercise main_spark
from pyspark import SparkContext

import sys
import os

def basic(numbers):
    squares = []
    for number in numbers:
        if number < 4:
            squares.append(number*number)
    print "Result of the local calculation: ", squares

def basic_pythonic(numbers):
    squares = map(lambda x: x*x, filter(lambda x: x < 4, numbers))
    print "Result of the local pythonic calculation: ", squares

def main_local(args):
    numbers = [1,2,3,4,5]

    #Exercise: from Python to PySpark
    basic(numbers)

    #Before re-writing it in PySpark, re-write it using map and filter expressions
    basic_pythonic(numbers)

def main_spark(args):
    numbers = [1,2,3,4,5]

    #Now do with PySpark
    sc = SparkContext(appName="MyFirstApp")
    numbers_rdd = sc.parallelize(numbers)
    squares_rdd = numbers_rdd.filter(lambda x: x < 4).map(lambda x: x*x)
    squares_rdd.repartition(1).saveAsTextFile("file://"+os.environ.get('SCRATCH_PATH')+"/output_exercise/")
    sc.stop()

if __name__=='__main__':
    #Run this locally first as python exercise.py
    main_local(sys.argv)

    #Do not run this example from the command line: use slurm.cmd
    #Need to uncomment the pyspark import line at the top
    #And submit as sbatch slurm.cmd
    #main_spark(sys.argv)

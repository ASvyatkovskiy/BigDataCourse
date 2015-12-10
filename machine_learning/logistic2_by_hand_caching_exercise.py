#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
import numpy as np
import time
import sys

N = 10**6
fraction_positive = 0.5

def y(x):
    return 1 if x < fraction_positive else 0

def generate_sample():
    sample_X = np.arange(0, 1, 1.0/N)
    np.random.shuffle( sample_X) # In-place shuffle!
    sample_Y = map(y, sample_X)
    return (sample_X, sample_Y)

def main(args,niter,nparts):
    sc = SparkContext(appName="LogisticByHand")

    (sample_X, sample_Y) = generate_sample()

    ## By hand.  This is the example code taken from the Spark Examples on the website.
    #  This is much slower than the above code, so I'm not going to even run it (or extract predictions, or test it..)
    start = time.time()

    def logistic_by_hand(ITERATIONS,nparts):
        #FIXME turn the caching on and off
        points = sc.parallelize( zip(sample_X, sample_Y),nparts).map(lambda (x,y): LabeledPoint(y, [1, x])).cache()
        w = np.random.ranf(size = 2) # current separating plane
        print "Original random plane: %s" % w
        for i in xrange(ITERATIONS):
            gradient = points.map(
                lambda pt: (1 / (1 + np.exp(-pt.label*(w.dot(pt.features)))) - 1) * pt.label * pt.features
            ).reduce(lambda a, b: a + b)
            w -= gradient
        print "Final separating plane: %s" % w


    logistic_by_hand(niter,nparts)
    end = time.time()
    print "Elapsed time: ", (end-start), " number of partitions was: ",nparts, " and number of iterations was: ", niter

if __name__=='__main__':
    #Try 40 and 80
    niterations = 40
    #Use the value for the number of partitions which gave you the best perfromance in the previous exercise
    npartitions = ...
    main(sys.argv,niterations,npartitions)

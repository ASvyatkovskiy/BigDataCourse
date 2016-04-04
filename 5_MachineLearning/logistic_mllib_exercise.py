#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
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
    sc = SparkContext(appName="LogisticMLlib")

    (sample_X, sample_Y) = generate_sample()

    start = time.time()

    points = sc.parallelize( zip(sample_X, sample_Y), nparts).map(lambda (x,y): LabeledPoint(y, [1, x])).cache()
    model = LogisticRegressionWithSGD.train(points,iterations=niter)

    end = time.time()
    print "Elapsed time: ", (end-start), " number of partitions was: ",nparts, " and number of iterations was: ", niter

    # Evaluating the model on training data
    labelsAndPreds = points.map(lambda p: (p.label, model.predict(p.features)))
    trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(points.count())
    print "Accuracy on training set: %s" % (1 - trainErr)

if __name__=='__main__':
    niterations = 40
    npartitions = ...
    #Use your best values of nuber of partitions to run on the MLlib version
    main(sys.argv,niterations,npartitions)

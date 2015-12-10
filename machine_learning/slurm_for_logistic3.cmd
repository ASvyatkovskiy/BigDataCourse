#!/bin/bash

#SBATCH -N 1
#SBATCH -t 00:20:00
#SBATCH --ntasks-per-node 6
#SBATCH --cpus-per-task 3

module load python
module load spark/hadoop2.6/1.4.1
spark-start
echo $MASTER

spark-submit --total-executor-cores 18 logistic3_mllib_exercise.py

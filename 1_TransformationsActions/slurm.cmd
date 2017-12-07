#!/bin/bash

#SBATCH -N 1
#SBATCH -t 00:05:00
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task 1
#SBATCH --reservation bigdata_introduction

module load python
module load spark/hadoop2.7/2.2.0
spark-start
echo $MASTER

spark-submit --total-executor-cores 1 exercise.py 

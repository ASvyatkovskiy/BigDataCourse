#!/bin/bash

#SBATCH -N 1
#SBATCH -t 00:20:00
#SBATCH --ntasks-per-node 6
#SBATCH --cpus-per-task 3
#SBATCH --reservation bigdata_introduction

module load python
module load spark/hadoop2.6/2.1.0
spark-start
echo $MASTER

spark-submit --total-executor-cores 18 --executor-memory 3G cache_partition.py

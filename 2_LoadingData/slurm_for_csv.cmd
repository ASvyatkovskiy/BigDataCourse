#!/bin/bash

#SBATCH -N 1
#SBATCH -t 00:05:00
#SBATCH --ntasks-per-node 2
#SBATCH --cpus-per-task 3
#SBATCH --reservation bigdata_introduction

module load python
module load spark/hadoop2.6/2.0.0
spark-start
echo $MASTER

spark-submit --total-executor-cores 6 --jars lib/spark-csv_2.10-1.3.0.jar,lib/commons-csv-1.2.jar load_csv_exercise.py

#!/bin/bash

#SBATCH -N 1
#SBATCH -t 00:20:00
#SBATCH --ntasks-per-node 6
#SBATCH --cpus-per-task 3
#SBATCH --reservation SparkClass

module load python
module load spark/hadoop2.7/2.2.0
spark-start
echo $MASTER

spark-submit --total-executor-cores 18 ml_pipeline.py

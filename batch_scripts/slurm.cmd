#!/bin/bash

#SBATCH -N 3
#SBATCH -t 24:00:00
#SBATCH --mem=20480
#SBATCH --ntasks-per-node 4
#SBATCH --cpus-per-task 5

module load spark/hadoop2.6/1.4.1 
spark-start
echo $MASTER

spark-submit --total-executor-cores 60 --executor-memory 5G target/scala-2.10/SimpleProject-assembly-1.0.jar

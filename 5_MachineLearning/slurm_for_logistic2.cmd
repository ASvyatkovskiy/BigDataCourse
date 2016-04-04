#!/bin/bash

#SBATCH -N 1
#SBATCH -t 00:20:00
#SBATCH --ntasks-per-node 6
#SBATCH --cpus-per-task 3
#SBATCH --reservation root_21

module load python
module load spark/hadoop2.6/1.6.1
spark-start
echo $MASTER

spark-submit --total-executor-cores 18 logistic2_by_hand_caching_exercise.py

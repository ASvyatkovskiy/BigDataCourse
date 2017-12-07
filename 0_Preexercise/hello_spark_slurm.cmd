#!/bin/bash

#SBATCH -N 1
#SBATCH -t 00:05:00
#SBATCH --ntasks-per-node 1

module load spark/hadoop2.7/2.2.0
spark-start
echo $MASTER

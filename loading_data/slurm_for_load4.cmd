#!/bin/bash

#SBATCH -N 1
#SBATCH -t 00:05:00
#SBATCH --ntasks-per-node 2
#SBATCH --cpus-per-task 3
#SBATCH --reservation root_21

module load python
module load spark/hadoop2.6/1.4.1
spark-start
echo $MASTER

export SPARK_HOME=/usr/licensed/spark/spark-1.4.1-bin-hadoop2.6/

spark-submit --total-executor-cores 6 --jars $SPARK_HOME/lib/spark-csv_2.10-1.3.0.jar,$SPARK_HOME/lib/commons-csv-1.2.jar load4_json.py

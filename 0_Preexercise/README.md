# Pre-exercises

Once you have set up the Docker or installed Spark and Python locally, proceed to complete KNN and Pandas exercises.

All of the pre-exercises should be completed on your laptops.

The pre-exercises are intended to build some domain knowledge in the fields covered during the course.
The interactive Jupyter notebooks cover machine learning (in particular, k-nearest neighbour classifier) and some modern data structures (Pandas DataFrame).

## Preparing for cluster portion of the mini-course

### How to run Spark on a Princeton University cluster: reading

To get a feeling of how this works, please look through the following FAQ page:
https://www.princeton.edu/researchcomputing/faq/spark-via-slurm/

Assuming you have an Adroit computing account, proceed to login to the cluster and checkout the mini-course repository.

```bash
git clone https://github.com/ASvyatkovskiy/BigDataCourse && cd BigDataCourse 
```

Submit a test Spark-slurm job (which only starts the Spark cluster and does nothing else):
```bash
cd 0_Preexercise
sbatch hello_spark_slurm.cmd
```

Check that your job got submitted:
```bash
squeue -u <your_username>
```

Look for the Slurm output file in the submission folder:
```bash
ls -l slurm-*.out
```
and inspect it with your favourite text editor. The last line in the log file will give you the name of the Spark master node.

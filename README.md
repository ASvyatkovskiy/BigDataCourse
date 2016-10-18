#Introduction to Big Data with Apache Spark

Go to the root of work area where you checked out the course pre-exercises, and type:
```bash
git checkout Fall2016
```

If you have not follow the pre-exercises, then check-out the git repository with the exercise 

```bash
git clone https://github.com/ASvyatkovskiy/BigDataCourse
```
and then switch to the Spring2016 branch.

Next, activate your Anaconda environment where the iPython noteboom is installed:

```bash
#conda create --name my_conda --file conda-requirements.txt
source activate my_conda
```

Test that the necessary Spark environmental variables are set
```bash
echo $SPARK_HOME
```
(that should return a valid path in your filesystem, as opposed to an empty string)

Locate your anaconda python:
```bash
which python
```
this query will return something like `/path/to/my/anaconda/bin/python`, copy the path up to the `bin` folder.

Set environmental variables to launch Spark-enabled jupyter notebook.
```bash
export PYSPARK_DRIVER_PYTHON="/path/to/my/anaconda/bin/jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"
export PYSPARK_PYTHON="/path/to/my/anaconda/bin/python"
```
and add them to your `.profile` so that you do not need to retype every time you open a command line window. 

Next launch of pyspark shell will prompt you to the notebook:
```bash
pyspark [options]
```
where [options] is the list of flags you pass to pyspark.
change to the directory for first task 1_TransformationsActions in the web browser GUI, and click on the interactive python file.

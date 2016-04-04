In this section, we will explore loading basic data types into RDDs and Dataframes.

Let us copy some additional jars to be able to use a third-part spark-csv library from Databricks. From the root folder of the exercises do:

```bash
cd BigDataCourse
export WORKDIR_ROOT=$PWD
cp $WORKDIR_ROOT/ExtraJars/* $SPARK_HOME/lib_managed/jars/
```

Now, restart your notebook like this:

```bash
IPYTHON_OPTS="notebook" $SPARK_HOME/bin/pyspark --jars $SPARK_HOME/lib_managed/jars/spark-csv_2.10-1.3.0.jar,$SPARK_HOME/lib_managed/jars/commons-csv-1.2.jar
```

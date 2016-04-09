In this section, we will explore loading basic data types into RDDs and Dataframes.

On your laptops, where you will run all of the iPython notebook, go ahead and copy some additional jars to be able to use a third-part spark-csv library from Databricks (note, that you do not have to do it on the cluster - it is taken care of for you). From the root folder of the exercises do:

```bash
cd BigDataCourse
cp ExtraJars/* $SPARK_HOME/lib_managed/jars/
```

Now, restart your notebook like this:

```bash
IPYTHON_OPTS="notebook" $SPARK_HOME/bin/pyspark --jars $SPARK_HOME/lib_managed/jars/spark-csv_2.10-1.3.0.jar,$SPARK_HOME/lib_managed/jars/commons-csv-1.2.jar
```

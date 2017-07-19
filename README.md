# Advanced Data Analysis Techniques with Apache Spark

## Getting ready for the workshop (required before the day of workshop)

You will need to request access to the Adroit cluster, install Apache Spark and some packages on your laptop, and optionally complete a set of pre-exercises.

All the pre-exercises are supposed to be completed on your laptops. Some of the workshop exercises will be done on your laptops, and some will be done on the cluster.

### Request an Adroit computing account

Please make sure you have and Adroit computing account or request it following the instructions on the page:
https://www.princeton.edu/researchcomputing/computational-hardware/adroit/

#### Test connection to the cluster

If you have the account, login to Adroit with X11 forwarding enabled:

```bash
ssh -XC your_username@adroit3.princeton.edu
```

Install ssh client on your laptop if necessary (for instance, Putty).

#### How to run Spark on a Princeton University cluster: reading

To get a feeling of how this works, please look through the following FAQ page:
https://www.princeton.edu/researchcomputing/faq/spark-via-slurm/

# Install with Docker

[Docker](https://www.docker.com/) is a containerisation engine that makes it much easier to run softwares. It essentially works as a lightweight VM manager, allowing you to spin instances up or down very easily. First step is to [install Docker](https://www.docker.com/community-edition).

Once you have Docker, run the following to download all of the images required to spin up a Spark Notebook:
```bash
docker pull jupyter/all-spark-notebook
```
This will take a little while, but once it's complete you are basically ready to go. Clone this repo, switch to the Spring2017 branch, and run a Docker container with the following commands:
```bash
git clone https://github.com/ASvyatkovskiy/BigDataCourse && cd BigDataCourse
git checkout Spring2017
docker run -it --rm -p 8888:8888 -v $(pwd)/:/home/jovyan/work jupyter/all-spark-notebook
```
Enter the URL that pops up in your terminal into a browser, and you should be good to go.

# Install Locally


### Install Apache Spark on your laptop

Install Apache Spark 2.1.0 on your laptop. Here are some instructions on how to do that:
1) Go to the Spark download page: http://spark.apache.org/downloads.html
select installation from source: http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0.tgz

Download, unpack, and build it with Maven:

```bash
#cd spark-2.1.0/
build/mvn -DskipTests clean package
```

You are going to need to install Maven build tool for that. 
Maven can be downloaded from the web: https://maven.apache.org/download.cgi unpack it, and add to the `PATH`:
```bash
export PATH=<path to your maven>/apache-maven-3.3.9/bin:$PATH
```

2) Update following environmental variables to point to the new Spark location:

```bash
export SPARK_HOME=/home/<your_username>/your_local_spark/spark-2.1.0
export PATH=$SPARK_HOME/sbin:$SPARK_HOME/bin:$PATH
```

these lines should be added to the *.bashrc* file on your laptop, otherwise you would have to export these values each time you log in!

### Download and install Anaconda

Please go to the following website: https://www.continuum.io/downloads
download and install *the latest* Anaconda version for Python 2.7 for your operating system. 

Note: we are going to need Anaconda 4.1.x or later

After that, type:

```bash
conda --help
```
and read the manual.
Once Anaconda is ready, download the following requirements file: https://github.com/ASvyatkovskiy/BigDataCourse/blob/master/preexercise/conda-requirements.txt
and proceed with setting up the environment:

```bash
conda create --name my_conda --file conda-requirements.txt
source activate my_conda
```

### Install git

If you do not have it installed already, install git following the instructions on that page: https://git-scm.com/book/en/v2/Getting-Started-Installing-Git
and proceed to checkout the repository for the course.

### Check-out the git repository with the pre-exercise 

```bash
git clone https://github.com/ASvyatkovskiy/BigDataCourse
cd BigDataCourse/preexercise/
```

Start interactive jupyter (ipython) notebook:
```bash
jupyter notebook
```
and proceed to complete each of the pre-exercises one by one.


## Preparing for cluster portion of the mini-course

Assuming you have an Adroit computing account, proceed to login to the cluster and checkout the mini-course repository.

```bash
git clone https://github.com/ASvyatkovskiy/BigDataCourse 
cd BigDataCourse/preexercise
```

Submit a test Spark-slurm job (which only starts the Spark cluster and does nothing else):
```bash
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

## Pre-exercises (not required, but might be interesting)

All of the pre-exercises should be completed on your laptops.

The pre-exercises are intended to build some domain knowledge in the fields covered during the course. 
The interactive iPython notebooks cover web-mining (scraping), text processing, elements of natural language processing, machine learning (in particular, k-nearest neighbour classifier) and some modern data structures (Pandas DataFrame). 
Each of these pre-exercises is supposed to be completed on the laptop and does not require cluster access, Apache Hadoop or Apache Spark installed.

The pre-exercises cover some of the topics that will be discussed during the main course in detail.

## Additional installations

### Pre-requisites

First, ensure you got the right version of jupyter and Java by typing: 

```bash
jupyter --version
``` 
it should print a value >= 4.0, and 

```bash
java -version
```
it should print a value like "1.8.0". If have version 1.7 on your laptops then go to http://download.oracle.com website and download it. Here is the direct link for Mac users: http://download.oracle.com/otn-pub/java/jdk/8u101-b13/jdk-8u101-macosx-x64.dmg

Finally, check the SPARK_HOME environmental variable is set:

```bash
echo $SPARK_HOME
```
(should return a non empty string on the screen)

### Install Apache Toree

If all the prerequisites are in order, proceed to install Apache Toree - it provides a Scala Spark kernel for Jupyter.

```bash
#cd BigDataCourse
pip install --user Toree/toree-0.2.0.dev1.tar.gz
```

Configure Apache Toree installation with Jupyter:
```bash
jupyter toree install --spark_home=$SPARK_HOME
```
If you get an error message like: `jupyter: 'toree' is not a Jupyter command`, then you would need to set:
```bash
export PATH=~/.local/bin:$PATH
```
and re-issue the command.

Confirm installation:
```bash
jupyter kernelspec list
```
You should see something like:
```bash
Available kernels:
  python2               /Users/alexey/anaconda/envs/BDcourseFall2016/lib/python2.7/site-packages/ipykernel/resources
  apache_toree_scala    /usr/local/share/jupyter/kernels/apache_toree_scala
```

Next launch of `jupyter notebook` will give you an option to choose Apache Toree kernel from the upper right menu, which supports Scala and Spark.

## In case help needed
If you are experiencing any problems with the installation part or pre-exercises: please email me at alexeys@princeton.edu or come see me at the regular CSES office hours on Tuesday.


# Apache Spark mini-course

Go to the root of work area where you checked out the course pre-exercises, and type:
```bash
git checkout Spring2017
```

If you have not follow the pre-exercises, then check-out the git repository with the exercise 

```bash
git clone https://github.com/ASvyatkovskiy/BigDataCourse
```
and then switch to the Spring2017 branch.

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

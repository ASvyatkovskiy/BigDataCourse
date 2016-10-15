#Introduction to Big Data with Apache Spark

## Getting ready for the workshop (required before the day of workshop)

You will need to request access to the Adroit cluster, install Apache Spark and some packages on your laptop, and optionally complete a set of pre-exercises.

All the pre-exercises are supposed to be completed on your laptops. Some of the workshop exercises will be done on your laptops, and some will be done on the cluster.

### Request an Adroit computing account

Please make sure you have and Adroit computing account or request it following the instructions on the page:
https://www.princeton.edu/researchcomputing/computational-hardware/adroit/

### Test connection to the cluster

If you have the account, login to Adroit with X11 forwarding enabled:

```bash
ssh -XC your_username@adroit3.princeton.edu
```

Install ssh client on your laptop if necessary (for instance, Putty).

### How to run Spark on a Princeton University cluster: reading

To get a feeling of how this works, please look through the following FAQ page:
https://www.princeton.edu/researchcomputing/faq/spark-via-slurm/


### Install Apache Spark on your laptop

Install Apache Spark 2.0.0 on your laptop. Here are some instructions on how to do that:
1) Go to the Spark download page: http://spark.apache.org/downloads.html
select a prebuilt distribution: http://d3kbcqa49mib13.cloudfront.net/spark-2.0.0-bin-hadoop2.6.tgz

Download and unpack it, then proceed to the step 2). Alternatively, one can install and build a custom Spark distribution from source:

```bash
git clone git://github.com/apache/spark.git
cd spark/
build/mvn -DskipTests clean package
```

You are going to need to install Maven build tool for that. 
Maven can be downloaded from the web: https://maven.apache.org/download.cgi unpack it, and add to the `PATH`:
```bash
export PATH=<path to your maven>/apache-maven-3.3.9/bin:$PATH
```

2) Update following environmental variables to point to the new Spark location:

```bash
export SPARK_HOME=/home/<your_username>/your_local_spark/spark
export PATH=$SPARK_HOME/sbin:$SPARK_HOME/bin:$PATH
```

these lines should be added to the *.bashrc* file on your laptop, otherwise you would have to export these values each time you log in!

## Pre-exercises (not required, but might be interesting)

The pre-exercises are intended to build some domain knowledge in the fields covered during the course. 
The interactive iPython notebooks cover web-mining (scraping), text processing, elements of natural language processing, machine learning (in particular, k-nearest neighbour classifier) and some modern data structures (Pandas DataFrame). 
Each of these pre-exercises is supposed to be completed on the laptop and does not require cluster access, Apache Hadoop or Apache Spark installed.

The pre-exercises cover some of the topics that will be discussed during the main course in detail.

## Getting started with pre-exercises

All exercises are intended to be perfromed on your laptops. 

### Download and install Anaconda

Please go to the following website: https://www.continuum.io/downloads
download and install Anaconda version for Python 2.7 for your operating system. After that, type:

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

### Installing Jupyter Scala launcher

First, ensure you got the right version of jupyter by typing: 

```bash
jupyter --version
``` 
it should print a value >= 4.0. Next, download and run the Jupyter Scala launcher with:

```bash
curl -L -o jupyter-scala https://git.io/vrHhi && chmod +x jupyter-scala && ./jupyter-scala && rm -f jupyter-scala
```

This downloads the bootstrap launcher of Jupyter Scala, then runs it. 
If no previous version of it is already installed, this simply sets up 
the kernel in ~/Library/Jupyter/kernels/scala211 (OSX) or ~/.local/share/jupyter/kernels/scala211 (Linux). 
Note that on first launch, it will download its dependencies from Maven repositories. 
These can be found under ~/.jupyter-scala/bootstrap.

Once installed, the downloaded launcher can be removed, as it copies itself 
in ~/Library/Jupyter/kernels/scala211 or ~/.local/share/jupyter/kernels/scala211.


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


#### Check-out the course github repository:

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


### In case help needed
If you are experiencing any problems with the installation part or pre-exercises: please email me at alexeys@princeton.edu or come see me at the regular CSES office hours on Tuesday.

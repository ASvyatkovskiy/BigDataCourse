#Introduction to Big Data with Apache Spark

## Pre-exercises

The pre-exercises are intended to build some domain knowledge in the fields covered during the course. 
The interactive iPython notebooks cover web-mining (scraping), text processing, elements of natural language processing, machine learning (in particular, k-nearest neighbour classifier) and some modern data structures (Pandas DataFrame). 
Each of these pre-exercises is supposed to be completed on the laptop and does not require cluster access, Apache Hadoop or Apache Spark installed.

The pre-exercises cover some of the topics that will be discussed during the main course in detail.

## Getting started with pre-exercises

All exercises are intended to be perfromed on your laptops. In addition, there is an exercise to test if you can connect to the computing cluster and start the Spark shell.

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
conda create --name alexeys_conda --file conda-requirements.txt
source activate alexeys_conda
```
please feel free to change the anaconda environment name.

### Install git and iPython
If you do not have it installed already, install git following the instructions on that page: https://git-scm.com/book/en/v2/Getting-Started-Installing-Git
and proceed to checkout the repository for the course.
In addition, you are going to need to install the iPython: http://ipython.org/install.html

### Check-out the git repository with the pre-exercise 

```bash
git clone https://github.com/ASvyatkovskiy/BigDataCourse
cd BigDataCourse/preexercise/
```

Start interactive ipython notebook:
```bash
ipython notebook
```
and proceed to complete each of the pre-exercises one by one.


### Connecting to the cluster

All the pre-exercises are supposed to be completed on your laptops. After that, please make sure you have and Adroit computing account or request it following the instructions on the following page:
https://www.princeton.edu/researchcomputing/computational-hardware/adroit/

If you have the account, do the following few steps:

#### Login to Adroit with X11 forwarding enabled:

```bash
ssh -XC your_username@adroit3.princeton.edu
```

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

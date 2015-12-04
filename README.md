#Introduction to Big Data with Apache Spark

## Pre-exercises

The pre-exercises are intended to build some domain knowledge in the fields covered during the course. 
The notebooks cover web-mining (scraping), NLP and text processing, machine learning (kNN) and some modern data structures (Pandas DataFrame). Each of these pre-exercises is supposed to be completed on the laptop and does not require cluster access or Apache Spark installed.

These are the topics that will be discussed during the main course at scale. 

## Getting started with pre-exercises

All exercises are intended to be perfromed on your laptops. In addition, there is an exercises to test if you can connect to the computing cluster and start the Spark shell.

### Download and install Anaconda

Please go to the following website: https://www.continuum.io/downloads
download and install Anaconda version for Python 2.7 for your operating system. After that, type

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

### Install git

If you do not have it installed already, install git following the instructions on that page: https://git-scm.com/book/en/v2/Getting-Started-Installing-Git
and proceed to checkout the repository for the course.

### Check-out the git repository for the exercise 

```bash
git clone https://github.com/ASvyatkovskiy/BigDataCourse
cd BigDataCourse/preexercise/
```


### In case help needed

If you are experiencing any problems with the installation part or pre-exercises: please come see me at the regular CSES office hours on Tuesday.

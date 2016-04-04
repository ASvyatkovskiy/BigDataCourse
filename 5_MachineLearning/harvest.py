#!/usr/bin/env python

import glob
from subprocess import Popen

logfiles = glob.glob("slurm*out")
map(lambda fname: Popen("cat "+fname+"| grep Elapsed",shell=True).wait(),logfiles)

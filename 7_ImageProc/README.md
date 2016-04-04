# Image processing with Thunder (on the Adroit cluster)

We are going to explore a single-node setup for this exercise. 
To run on a single node on Adroit you would have to first install thunder:

```bash
cd BigDataCourse/7_ImageProc
module load python
pip install --user thunder
```

Once thunder is installed you can use it in jobs after adding to your
Slurm scripts:

```bash
module load python
module load spark/hadoop2.6/1.6.1
export PATH=~/.local/bin:$PATH
```

#!/usr/bin/python

#Description:
#---------------------
#get outliers using high contrast subspaces and LOF
#---------------------
#Create by coder-james

import os
import lib.conf as conf
from lib.lof_ac import LOF
import numpy as np
from time import time
import lib.hics as hics
import pandas as pd

inputfile="data_red_filter.csv"
lof_file="lof_value.txt"

def getDataFrame():
  """read csv to get dataframe using pandas"""
  filepath = os.path.join(conf.DATA_DIR, inputfile)
  df = pd.read_csv(filepath)
  return df

def process():
  df = getDataFrame()
  #nums = ids.shape[0]
  t1 = time()
  """get all selected subspaces based on high contrast subspace"""
  #Kolmogorov_Smirnor / Welch Test
  subspaces = hics.selection(df.iloc[:,1:])
  #Pearson
  subspaces = hics.p_selection(df.iloc[:,1:])
  print subspaces
  print "%s s --- select subspaces" %(time() - t1)
  """ choose to compute local density """
  #k = 50
  #hics_lof(k, vectors)
  #print "%s s --- %s compute %s records" %(time() - t1, method, nums)

def hics_lof(k, vectors):
  """local outlier factor sample"""
  m = LOF(k, include=False)
  lofs = m.fit(vectors)
  with open(os.path.join(conf.OUTPUT_DIR, lof_file), "w") as loff:
    content = ""
    for value in lofs:
      content += "%.3f;" % value
    loff.write(content[:-1])
  
def check():
  """check lof value to tune"""
  with open(os.path.join(conf.OUTPUT_DIR, lof_file)) as loff:
    lines = np.array([float(line) for line in loff.read().split(";") if len(line) > 0])
    threshold = 4
    subs = lines > threshold
    indices, ids, vectors = getvecs()
    outliers = []
    for i,isoutlier in enumerate(subs):
      if isoutlier:
        outliers.append(vectors[i])
    for j,name in enumerate(indices):
      content = "%-35s" % name
      for outlier in outliers:
        content += "%-10s" % outlier[j]
      print content

if __name__ == "__main__":
  process()
  #check()

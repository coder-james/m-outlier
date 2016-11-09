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

def getDataFrame(inputfile):
  """read csv to get dataframe using pandas"""
  filepath = os.path.join(conf.DATA_DIR, inputfile)
  df = pd.read_csv(filepath)
  return df

def process(inputfile):
  df = getDataFrame(inputfile)
  #nums = ids.shape[0]
  print list(df.columns)
  t1 = time()
  """get all selected subspaces based on high contrast subspace"""
  #Kolmogorov_Smirnor / Welch Test
  subspaces = hics.selection(df.iloc[:,1:])
  #Pearson
  #subspaces = hics.p_selection(df.iloc[:,1:])
  col = {}
  for spaces in subspaces:
    for item in spaces:
      if item in col:
        col[item]+=1
      else:
        col[item]=1
  print col
  print subspaces
  print len(subspaces)
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

if __name__ == "__main__":
  #inputfile = "data_spark_task_metrics_summary_filter.csv"
  inputfile = "data_hdfs_audit_filter.csv"
  #inputfile = "data_job_metrics_summary_filter.csv"
  process(inputfile)

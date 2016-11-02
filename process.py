#!/usr/bin/python

#Description:
#---------------------
#process data to get outliers
#---------------------
#Create by coder-james

import os
import lib.conf as conf
from lib.lof_ac import LOF
import numpy as np
from time import time

inputfile="data_10mon.txt"
outputfile="data_10mon_filter.txt"
redfile="data_red_filter.txt"
lof_file="lof_value.txt"
prefix_list=["app_id","app_start_time","app_finish_time","app_name","user_sk","job_id","stage_id","create_time", \
	   "spark_scheduler_mode","spark_streaming_internal_batch_time","stage_name","stage_attempt_id", \
	   "app_attempt_id"]
same_=["duration","jvm_gc_time","result_serialization_time","getting_result_time","executor_deserialize_time","scheduler_delay"]
#suffix=["_min","_25th_percentile","_median","_75th_percentile","_95th_percentile","_99th_percentile","_max"]
suffix=["_25th_percentile","_median","_75th_percentile","_95th_percentile","_99th_percentile"]

def getvecs(filename):
  with open(os.path.join(conf.DATA_DIR, filename)) as inputf:
    lines = np.array([np.array(line.split(",")) for line in inputf.read().split("\n") if len(line) > 0])
    indices = lines[0,1:]
    ids = lines[1:,0]
    vectors = lines[1:,1:]
  return indices,ids,vectors

def process(method="kmeans"):
  indices,ids,vectors = getvecs(outputfile)
  nums = ids.shape[0]
  t1 = time()
  """ choose to compute local density """
  k = 50
  if method == "kmeans":
    kmeans(vectors)
  elif method == "lof":
    lof(k, vectors)
  print "%s s --- %s compute %s records" %(time() - t1, method, nums)

def lof(k, vectors):
  """local outlier factor sample"""
  m = LOF(k, include=False)
  lofs = m.fit(vectors)
  with open(os.path.join(conf.OUTPUT_DIR, lof_file), "w") as loff:
    content = ""
    for value in lofs:
      content += "%.3f;" % value
    loff.write(content[:-1])

def filter(filename):
  """filter some indices to reduce non-integer value and unrelevant columns"""
  t1 = time()
  clear_list = []
  for name in prefix_list:
    clear_list.append(name)
  for sname in same_:
    for sfx in suffix:
      clear_list.append(sname + sfx)
  with open(os.path.join(conf.DATA_DIR, inputfile)) as datafile:
    lines = [line for line in datafile.read().split("\n") if len(line) > 0]
    index = {name:i for i,name in enumerate(lines[0].split(","))}
    for clear in clear_list:
      assert clear in index
    keep_indices = sorted([value for key,value in index.items() if key not in clear_list])
    #data = []
    with open(os.path.join(conf.DATA_DIR, filename), "w") as outfile:
      matrix = []
      for i,line in enumerate(lines):
        items = np.array(line.split(","))[keep_indices]
        if i == 0:
          top = items
        else:
          matrix.append(np.array(items, np.float32))
      mat = np.array(matrix)
      keep = []
      for i,_ in enumerate(top):
        if mat[:,i].mean() != 0 and mat[:,i].var() != 0:
          keep.append(i)
      top = top[keep]
      mat = mat[:,keep]
      outfile.write(",".join(map(str, top)) + "\n")
      for item in mat:
        outfile.write(",".join(map(str, item)) + "\n")
  print "%s s --- %s cols filter and manual rest %s and final rest %s" % (time() - t1, len(index), len(index) - len(clear_list), len(keep))
  
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

def examine(filename):
  """statistic for each attribute"""
  indices, ids, vecs = getvecs(filename)
  nvecs = np.array(vecs, np.float32)
  for i,name in enumerate(indices):
    print "%-42s|%-12s|%-12s|%s" %(name, nvecs[:,i].mean(), nvecs[:,i].var(), nvecs[:,i].std())
  print ids.shape
  print vecs.shape
    

if __name__ == "__main__":
  #process(method="lof")
  """
  scipy k neighbors    |    sikit-learn k neighbors
  --- 212.6442 s       |    --- 3.7189 s
  lof function...k distances
  --- 0.7437 s
  lof function...local reachability density
  --- 1.0844 s
  lof function...lof value compute
  --- 0.3548 s
  217.646481037 s --- lof compute 
  """
  #check()
  #filter(redfile)
  """
  0.662287950516 s --- 100 cols filter and manual rest 57 and final rest 33
  """
  examine(redfile)

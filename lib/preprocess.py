#!/usr/bin/python

#Description:
#---------------------
#Data Preprocess
#---------------------
#Create by coder-james

import os
import numpy as np
import conf
from time import time

def get_list(inputname):
  tablename = inputname[inputname.find("_") + 1: inputname.find(".")]
  useless_list = []
  if tablename == "spark_task_metrics_summary":
    nomial_list=["app_id","app_name","job_id","stage_id","create_time", "spark_scheduler_mode", \
               "spark_streaming_internal_batch_time","stage_name","stage_attempt_id", "app_attempt_id"]
    manual_list=["user_sk","app_start_time","app_finish_time"]
    same_=["duration","jvm_gc_time","result_serialization_time","getting_result_time","executor_deserialize_time","scheduler_delay"]
    suffix=["_sum","_variance","_min","_25th_percentile","_median","_75th_percentile","_95th_percentile","_99th_percentile","_max"]
    for name in nomial_list + manual_list:
      useless_list.append(name)
    for sname in same_:
      for sfx in suffix:
        useless_list.append(sname + sfx)
  elif tablename == "hdfs_audit":
    nomial_list=["time","user_id","ip","username","create_time"]
    for name in nomial_list:
      useless_list.append(name)
  elif tablename == "job_metrics_summary":
    nomial_list=["jobId","create_time","jobName","userName","queue","confPath","status","applicationType", \
                 "id"]
    manual_list=["launchTime","acls","maps","reduces","priority","finishedMaps","finishedReduces","failedMaps", \
                 "failedReduces","mapsRuntime","reducesRuntime","mapCounters","reduceCounters","jobCounters", \
                 "inputBytes","outputBytes","delay"]
    for name in nomial_list + manual_list:
      useless_list.append(name)
  return tablename,useless_list

def getvecs(filename, directory=conf.DATA_DIR):
  """transform file to vectors"""
  with open(os.path.join(directory, filename)) as inputf:
    lines = np.array([np.array(line.split(",")) for line in inputf.read().split("\n") if len(line) > 0])
    indices = lines[0,1:]
    ids = lines[1:,0]
    vectors = lines[1:,1:]
  return indices,ids,vectors

def filter(inputfile, norm=False):
  """filter some indices to reduce non-integer value and unrelevant columns"""
  t1 = time()
  tablename, clear_list = get_list(inputfile)
  indices, ids, vectors = getvecs(inputfile)
  index = {name:i for i,name in enumerate(indices)}
  for clear in clear_list:
    assert clear in index
  keep_indices = sorted([value for key,value in index.items() if key not in clear_list])
  #data = []
  outputfile = conf.filter_prefix + tablename + conf.filter_suffix
  with open(os.path.join(conf.DATA_DIR, outputfile), "w") as outfile:
    matrix = []
    for i,line in enumerate(vectors):
      items = np.array(line)[keep_indices]
      matrix.append(np.array(items, np.float32))
    top = np.array(indices)[keep_indices]
    mat = np.array(matrix)
    keep = []
    #eliminate zero-variance index
    for i,_ in enumerate(top):
      if mat[:,i].var() != 0:
        keep.append(i)
    top = top[keep]
    mat = mat[:,keep]
    #normalization
    if norm:
      for k in range(mat.shape[1]):
        mat[:,k] -= mat[:,k].mean()
        mat[:,k] /= mat[:,k].var() ** 0.5
    outfile.write("id,%s\n" % (",".join(map(str, top))))
    for i,item in enumerate(mat):
      outfile.write("%s,%s\n" %(ids[i],",".join(map(str, item))))
  print "%s s --- %s cols filtered and manual rest %s and final rest %s" % (time() - t1, len(index), len(index) - len(clear_list), len(keep))
  return outputfile

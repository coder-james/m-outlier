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
outputfile="data_10mon_filter.csv"
normfile="data_norm_filter.csv"
savefile="output.csv"
normsavefile="output_norm.csv"
lof_file="lof_value.txt"
norm_lof_file="lof_value_norm.txt"
nomial_list=["app_id","app_name","job_id","stage_id","create_time", "spark_scheduler_mode", \
	   "spark_streaming_internal_batch_time","stage_name","stage_attempt_id", "app_attempt_id"]
manual_list=["app_start_time","app_finish_time"]
same_=["duration","jvm_gc_time","result_serialization_time","getting_result_time","executor_deserialize_time","scheduler_delay"]
suffix=["_sum","_variance","_min","_25th_percentile","_median","_75th_percentile","_95th_percentile","_99th_percentile","_max"]
#suffix=["_sum","_25th_percentile","_median","_75th_percentile","_95th_percentile","_99th_percentile"]

def getvecs(filename):
  with open(os.path.join(conf.DATA_DIR, filename)) as inputf:
    lines = np.array([np.array(line.split(",")) for line in inputf.read().split("\n") if len(line) > 0])
    indices = lines[0,1:]
    ids = lines[1:,0]
    vectors = lines[1:,1:]
  return indices,ids,vectors

def process(method="kmeans",filename=None,loffile=None):
  indices,ids,vectors = getvecs(filename)
  nums = ids.shape[0]
  t1 = time()
  """ choose to compute local density """
  k = 100
  if method == "kmeans":
    kmeans(vectors)
  elif method == "lof":
    lof(k, vectors, loffile)
    #lof_one(k, indices, vectors)
  print "%s s --- %s compute %s records" %(time() - t1, method, nums)

def lof(k, vectors, loffile):
  """local outlier factor sample 
     compute all dimension
  """
  m = LOF(k, include=False)
  with open(os.path.join(conf.OUTPUT_DIR, loffile), "w") as loff:
      lofs = m.fit(vectors)
      content = ""
      for value in lofs:
          content += "%.3f;" % value
      loff.write(content[:-1])

def lof_one(k, indices, vectors):
  """local outlier factor sample 
     compute one dimension
  """
  m = LOF(k, include=False)
  with open(os.path.join(conf.OUTPUT_DIR, lof_file), "w") as loff:
    for i, name in enumerate(indices):
      lofs = m.fit(vectors[:, i].reshape(-1,1))
      content = "%s:" % name
      result = sorted([[i,value] for i,value in enumerate(lofs) if value > conf.lof_threshold],key=lambda item:item[1],reverse=True)
      for item in result:
          content += "%s,%.3f;" %(item[0], item[1])
      loff.write(content[:-1]+"\n")
  #    break

def filter(filename,norm=True):
  """filter some indices to reduce non-integer value and unrelevant columns"""
  t1 = time()
  clear_list = []
  for name in nomial_list + manual_list:
    clear_list.append(name)
  for sname in same_:
    for sfx in suffix:
      clear_list.append(sname + sfx)
  indices, ids, vectors = getvecs(inputfile)
  index = {name:i for i,name in enumerate(indices)}
  for clear in clear_list:
    assert clear in index
  keep_indices = sorted([value for key,value in index.items() if key not in clear_list])
  #data = []
  with open(os.path.join(conf.DATA_DIR, filename), "w") as outfile:
    matrix = []
    for i,line in enumerate(vectors):
      items = np.array(line)[keep_indices]
      matrix.append(np.array(items, np.float32))
    top = np.array(indices)[keep_indices]
    mat = np.array(matrix)
    keep = []
    for i,_ in enumerate(top):
      if mat[:,i].var() == 0:
        pass
      else:
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
  print "%s s --- %s cols filter and manual rest %s and final rest %s" % (time() - t1, len(index), len(index) - len(clear_list), len(keep))
  
def check(lofname, filename):
  """check lof value to tune"""
  with open(os.path.join(conf.OUTPUT_DIR, lofname)) as loff:
    lines = np.array([float(line) for line in loff.read().split(";") if len(line) > 0])
    subs = lines > conf.lof_threshold
    indices, ids, vectors = getvecs(filename)
    outliers = []
    no = []
    for i,isoutlier in enumerate(subs):
      if isoutlier:
        outliers.append(vectors[i])
        no.append([i, float(lines[i])])
    print sorted(no,key=lambda item: item[1], reverse=True)
    for j,name in enumerate(indices):
      content = "%-35s" % name
      for outlier in outliers:
        content += "%-12s" % outlier[j]
      print content

def rate(lofname, savefile):
  """relist all sample by lof value"""
  with open(os.path.join(conf.OUTPUT_DIR, lofname)) as loff:
    lines = np.array([float(line) for line in loff.read().split(";") if len(line) > 0])
  with open(os.path.join(conf.DATA_DIR, outputfile)) as inputf:
    ilines = [line for line in inputf.read().split("\n") if len(line) > 0]
  with open(os.path.join(conf.OUTPUT_DIR, savefile), "w") as sfile:
    lofs = sorted([[i, score] for i,score in enumerate(lines)],key=lambda item:item[1],reverse=True)
    sfile.write(ilines[0] + ",lof\n")
    for tup in lofs:
      sfile.write("%s,%s\n" %(ilines[tup[0] + 1], tup[1]))
  

def examine(filename):
  """statistic for each attribute"""
  indices, ids, vecs = getvecs(filename)
  nvecs = np.array(vecs, np.float32)
  for i,name in enumerate(indices):
    mean = nvecs[:,i].mean()
    var = nvecs[:,i].var()
    print "%-42s|%-12s|%s" %(name, mean, var)
    #nvecs[:,i] -= mean
    #nvecs[:,i] /= var ** 0.5
    #print "%-42s|%-12s|%s" %(name, nvecs[:,i].mean(), nvecs[:,i].var())
  print ids.shape
  print vecs.shape
    

if __name__ == "__main__":
  norm = True
  if norm:
    filename = normfile
    lof_filename = norm_lof_file
    save_filename = normsavefile
  else:
    filename = outputfile
    lof_filename = lof_file
    save_filename = savefile
  filter(filename, norm=norm)
  examine(filename)
  process(method="lof", filename=filename, loffile=lof_filename)
  #check(lof_file, outputfile)
  rate(lof_filename, save_filename)

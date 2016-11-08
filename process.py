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
import lib.preprocess as pre

def process(method="lof",filename=None):
  tablename = filename[5:-11]
  loffile = tablename+"_lof.txt"
  indices,ids,vectors = pre.getvecs(filename)
  nums = ids.shape[0]
  t1 = time()
  """ choose to compute local density """
  k = conf.get_k(tablename)
  if method == "kmeans":
    pass #wait for develop...
  elif method == "lof":
    lof(k, vectors, loffile)
    #lof_one(k, indices, vectors)
  print "%s s --- %s compute %s records" %(time() - t1, method, nums)
  return loffile

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
  
#def check(lofname, filename):
#  """check lof value to tune"""
#  with open(os.path.join(conf.OUTPUT_DIR, lofname)) as loff:
#    lines = np.array([float(line) for line in loff.read().split(";") if len(line) > 0])
#    subs = lines > conf.lof_threshold
#    indices, ids, vectors = getvecs(filename)
#    outliers = []
#    no = []
#    for i,isoutlier in enumerate(subs):
#      if isoutlier:
#        outliers.append(vectors[i])
#        no.append([i, float(lines[i])])
#    print sorted(no,key=lambda item: item[1], reverse=True)
#    for j,name in enumerate(indices):
#      content = "%-35s" % name
#      for outlier in outliers:
#        content += "%-12s" % outlier[j]
#      print content

def rate(lofname, outputfile):
  tablename = lofname[:-8]
  """relist all sample by lof value"""
  with open(os.path.join(conf.OUTPUT_DIR, lofname)) as loff:
    lines = np.array([float(line) for line in loff.read().split(";") if len(line) > 0])
  with open(os.path.join(conf.DATA_DIR, outputfile)) as inputf:
    ilines = [line for line in inputf.read().split("\n") if len(line) > 0]
  with open(os.path.join(conf.OUTPUT_DIR, tablename+"_output.csv"), "w") as sfile:
    lofs = sorted([[i, score] for i,score in enumerate(lines)],key=lambda item:item[1],reverse=True)
    sfile.write(ilines[0] + ",lof\n")
    for tup in lofs:
      sfile.write("%s,%s\n" %(ilines[tup[0] + 1], tup[1]))
  

def examine(filename):
  """statistic for each attribute"""
  indices, ids, vecs = pre.getvecs(filename)
  nvecs = np.array(vecs, np.float32)
  for i,name in enumerate(indices):
    mean = nvecs[:,i].mean()
    var = nvecs[:,i].var()
    print "%-42s|%-12s|%s" %(name, mean, var)
  print ids.shape, vecs.shape
    

if __name__ == "__main__":
  #inputfile = "data_spark_task_metrics_summary.txt"
  #inputfile = "data_hdfs_audit.txt"
  inputfile = "data_job_metrics_summary.txt"
  norm = False
  filename = pre.filter(inputfile, norm)
  examine(filename)
  loffile = process(method="lof", filename=filename)
  rate(loffile, filename)

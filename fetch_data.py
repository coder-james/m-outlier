#!/usr/bin/python

#Description:
#---------------------
#Fetch Data from Mysql Database
#---------------------
#Create by coder-james

import os.path as op
from lib.db import mysql
import lib.conf as conf
from time import time
import sys

#2016.11.1  Unix timestamp
end_date="14779584000" + "000"
#2016.10.1
start_date="1475280000" + "000"
#2016.9.1
last_date="1472688000" + "000"
#need to compute a new value
rawlist=["stage_submission_time","stage_completion_time","job_submission_time","job_completion_time"]

def fetch(tablenames):
  db = mysql()
  target = "*"
  if tablenames[0] == "spark_task_metrics_summary":
    wheres = ["app_start_time > " + last_date, "app_start_time < " + start_date]
    orderindex = "app_start_time"
    results = db.select(tablenames[0], target=target, wheres=wheres, orderindex=orderindex)
  elif tablenames[0] == "hdfs_audit":
    wheres = ["time > " + start_date, "time < " + end_date]
    orderindex = "time"
    results = db.select(tablenames[0], target=target, wheres=wheres, orderindex=orderindex)
  elif tablenames[0] == "job_metrics_summary":
    wheres = ["submitTime > %s" %(last_date), "submitTime < %s" % (start_date)]
    orderindex = "submitTime"
    key = "jobId"
    results = db.selectmulti(tablenames, target=target, key=key, wheres=wheres, orderindex=orderindex)
  else:
    print "no table (%s)" % tablenames[0]
    db.close()
    sys.exit()
  if results == None:
    print "no results"
  else:
    print "get %s records" % len(results)
    cols = conf.get_index(tablenames[0])
    if cols == None:
      print "no such table (%s)" % tablenames[0]
    else:
      #save(results, cols, tablenames)
      pass
  db.close()

def save(results, cols, tablenames):
  cols_dic = {name:i for i,name in enumerate(cols)}
  t1 = time()
  savefile="data_%s.txt" % tablenames[0]
  with open(op.join(conf.DATA_DIR, savefile), "w") as sfile:
    top_str = ""
    for colname in cols:
      if colname not in rawlist:
        top_str += colname + ","
    skip_index = []
    if tablenames[0] == "spark_task_metrics_summary":
      top_str +="stage_time,job_time\n"
      skip_index = [cols_dic[name] for name in rawlist]
    count={}
    sfile.write(top_str)
    for row in results:
      values = []
      for i,item in enumerate(row):
        if i not in skip_index:
          values.append(str(item))
        if tablenames[0] == "spark_task_metrics_summary":
          for i in range(0, len(rawlist), 2):
            va = long(row[cols_dic[rawlist[i + 1]]]) - long(row[cols_dic[rawlist[i]]])
            if va < 0:
              values.append("0")
              if i in count: count[i]+=1
              else: count[i]=1
            else:
    	      values.append(str(va))
      sfile.write(",".join(values) + "\n")
  print "%s s---%s records have been saved to %s" %(time()-t1, len(results), savefile)
  print count
  
if __name__ == "__main__":
  assert start_date < end_date
  tablenames = ["spark_task_metrics_summary"]
  #tablenames = ["hdfs_audit"]
  #tablenames = ["job_metrics_summary","job"]
  fetch(tablenames)

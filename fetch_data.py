#!/usr/bin/python

#Description:
#---------------------
#Fetch Data from Mysql Database
#---------------------
#Create by coder-james

import os.path as op
from lib.db import mysql
import lib.conf as conf
import time
import sys

def get_stamp(date_time):
  return "%s000" % long(time.mktime(time.strptime(date_time,"%Y:%m:%d %H")))

nov16=get_stamp("2016:11:1 8")
octo16=get_stamp("2016:10:1 8")
dec151=get_stamp("2015:12:1 8")
dec152=get_stamp("2015:12:1 16")
test1=get_stamp("2015:11:1 8")
test2=get_stamp("2016:10:1 8")
#need to compute a new value
addition={"spark_task_metrics_summary":"stage_time,job_time\n", "job_metrics_summary":"complete_time\n"}
rawlist={"spark_task_metrics_summary": ["stage_submission_time","stage_completion_time", \
        "job_submission_time","job_completion_time"], "job_metrics_summary": ["submitTime","finishTime"]}

def fetch(tablenames):
  db = mysql()
  target = "*"
  if tablenames[0] == "spark_task_metrics_summary":
    wheres = ["stage_submission_time > " + octo16, "stage_submission_time < " + nov16]
    orderindex = "stage_submission_time"
    results = db.select(tablenames[0], target=target, wheres=wheres, orderindex=orderindex)
  elif tablenames[0] == "hdfs_audit":
    wheres = ["time > " + dec151, "time < " + dec152]
    orderindex = "time"
    results = db.select(tablenames[0], target=target, wheres=wheres, orderindex=orderindex)
  elif tablenames[0] == "job_metrics_summary":
    wheres = ["submitTime > %s" %(test1), "submitTime < %s" % (test2)]
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
      save(results, cols, tablenames)
  db.close()

def save(results, cols, tablenames):
  cols_dic = {name:i for i,name in enumerate(cols)}
  t1 = time.time()
  savefile="data_%s.txt" % tablenames[0]
  with open(op.join(conf.DATA_DIR, savefile), "w") as sfile:
    top_str = ""
    for colname in cols:
      if tablenames[0] not in rawlist or tablenames[0] in rawlist \
                                  and colname not in rawlist[tablenames[0]]:
        top_str += colname + ","
    assert len(cols) == len(results[0])
    skip_index = []
    if tablenames[0] in addition:
      top_str += addition[tablenames[0]]
      skip_index = [cols_dic[name] for name in rawlist[tablenames[0]]]
      top_str 
    else:
      top_str = top_str[:-1]
      top_str +="\n"
    count={}
    sfile.write(top_str)
    for row in results:
      values = []
      for i,oitem in enumerate(row):
        if type(oitem) == str:
          item = oitem.replace(","," ").replace("\n"," ")
        else:
          item = str(oitem)
        if i not in skip_index:
          values.append(item)
      if tablenames[0] in rawlist:
        rlist = rawlist[tablenames[0]]
        for i in range(0, len(rlist), 2):
          va = long(row[cols_dic[rlist[i + 1]]]) - long(row[cols_dic[rlist[i]]])
          if va < 0:
            values.append("0")
            if i in count: count[i]+=1
            else: count[i]=1
          else:
            values.append(str(va))
      sfile.write(",".join(values) + "\n")
  print "%s s---%s records have been saved to %s" %(time.time()-t1, len(results), savefile)
  print count
  
if __name__ == "__main__":
  tablenames = ["spark_task_metrics_summary"]
  #fetch(tablenames)
  """16 10.1 - 11.1 get 15460 records"""
  tablenames = ["hdfs_audit"]
  #fetch(tablenames)
  """15 12.1.0 - 12.1.16 get 34778 records"""
  tablenames = ["job_metrics_summary","job"]
  #fetch(tablenames)
  """15.11.1 - 16.10.1 get 2580 records"""

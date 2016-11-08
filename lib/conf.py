#!/usr/bin/python

#Description:
#---------------------
#project configuration file
#---------------------
#Create by coder-james

DATA_DIR="data"
OUTPUT_DIR="output"
filter_prefix="data_"
filter_suffix="_filter.csv"

"""mysql configuration"""
mysql_ipaddr="10.31.73.48"
mysql_user="hue_user"
mysql_pwd="hue_test"
mysql_db="hue"

"""lof"""
lof_threshold=4

"""High Contrast Subspace"""
iteration=50
#Choose [Kolmogorov-Smirnov or Welch] Test to compute
test="kolmo" #welch
#test="welch" #welch
p_threshold=0.99
pearson_threshold=0.9


def get_index(tablename):
  with open("lib/indexes") as indexfile:
    content = indexfile.read().split("\n")
    dics = {line.split(":")[0]:line.split(":")[1].split(";") for line in content if len(line) > 0}
    if tablename in dics:
      return dics[tablename]
    else:
      return None

def get_k(tablename):
  if tablename == "spark_task_metrics_summary":
    return 100
  elif tablename == "hdfs_audit":
    return 1100
  elif tablename == "job_metrics_summary":
    return 50

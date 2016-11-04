#!/usr/bin/python

#Description:
#---------------------
#project configuration file
#---------------------
#Create by coder-james

DATA_DIR="data"
OUTPUT_DIR="output"

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
p_threshold=0.9
pearson_threshold=0.9

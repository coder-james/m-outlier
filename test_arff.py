#!/usr/bin/python

#Description:
#---------------------
#Fetch Data from arff file
#---------------------
#Create by coder-james

from scipy.io import arff
import lib.conf as conf
import os.path as op
import pandas as pd
import itertools
import numpy as np

datafile="test.arff"
data, meta = arff.loadarff(op.join(conf.DATA_DIR, datafile))
print data
"""
[(5.0, 3.25, 'blue') (4.5, 3.75, 'green') (3.0, 4.0, 'red')]
"""
print meta
"""
Dataset: foo
	width's type is numeric
	height's type is numeric
	color's type is nominal, range is ('red', 'green', 'blue', 'yellow', 'black')
"""
df = pd.DataFrame(data)
print df
"""
   width  height  color
0    5.0    3.25   blue
1    4.5    3.75  green
2    3.0    4.00    red
"""
print df.columns
"""
Index([u'width', u'height', u'color'], dtype='object')
"""
for item in itertools.combinations(list(df.columns),2):
  print item
"""
('width', 'height')
('width', 'color')
('height', 'color')
"""
print df.values
"""
[[5.0 3.25 'blue']
 [4.5 3.75 'green']
 [3.0 4.0 'red']]
"""
print (df.rank()/df.rank().max()).iloc[:,:-1]
print df[np.array([[True],[False],[True]])]
print df.values
a = [[1,2],[2,3],[1,2,3],[1,2,4]]
print [list(t) for t in set(map(tuple, a))]

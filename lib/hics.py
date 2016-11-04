#!/usr/bin/python

#Description:
#---------------------
#High Contrast Subspace
#---------------------
#Updated by coder-james

import itertools
import random
from scipy import stats
import numpy as np
import conf

def subGenerator(basespace, allspace, n):
 """generate possible subspace of n dimensions"""
 subspaces=[]
 for item in itertools.combinations(list(set(allspace) - set(basespace)),(n - len(basespace))):
   subspaces.append(sorted(basespace + list(item)))
 return subspaces

def selection(df):
  """get selected subspaces"""
  #start with 2-D subspaces
  subspaces = subGenerator([], df.columns, 2)
  tested=[]
  selection=[]
  index_df = (df.rank() / df.rank().max())
  #For each subspace that satisfies the cut_off point criteria
  #add additional dimensions
  while(len(subspaces)>0):
   if subspaces[0] not in tested:
     alpha1 = pow(0.2, (float(1) / float(len(subspaces[0]))))
     pvalue_Total = 0
     pvalue_cnt = 0
     avg_pvalue = 0
     for i in range(0, conf.iteration):
       lband = random.random()
       uband = lband + alpha1
       v = random.randint(0, (len(subspaces[0]) - 1))
       rest = list(set(subspaces[0]) - set([subspaces[0][v]]))
       if conf.test == "kolmo":
         """Kolmogorov-Smirnov test"""
         k = stats.ks_2samp(df[subspaces[0][v]].values, df[((index_df[rest] < uband) & \
                (index_df[rest] > lband)).all(axis = 1)][subspaces[0][v]].values)
       elif conf.test == "welch":
         """Welch T-test"""
         k = stats.ttest_ind(df[subspaces[0][v]].values, df[((index_df[rest] < uband) & \
                (index_df[rest] > lband)).all(axis = 1)][subspaces[0][v]].values)
       pvalue = k.pvalue
       if not(np.isnan(pvalue)):
         pvalue_Total += pvalue
         pvalue_cnt += 1
     if pvalue_cnt > 0:
       avg_pvalue = pvalue_Total / pvalue_cnt
     if (1.0 - avg_pvalue) > conf.p_threshold:
       print "subspaces %s, pvalue %s" %(subspaces[0],avg_pvalue)
       selection.append(subspaces[0])
       subspaces = subspaces + subGenerator(subspaces[0], df.columns, (len(subspaces[0]) + 1))
     tested.append(subspaces[0])
     subspaces.pop(0)
     subspaces = [list(t) for t in set(map(tuple, subspaces))]
   else:
     subspaces.pop(0)
  return selection

def p_selection(df):
  """get selected subspaces.
     The difference between `selection` function and this function is 
     the measurement of quality criterion for the subspace contrast
     and only select two-dimension subspace"""
  subspaces = subGenerator([], df.columns, 2)
  tested=[]
  selection=[]
  while(len(subspaces)>0):
    if subspaces[0] not in tested:
      if df[subspaces[0][0]].values.var() == 0 or df[subspaces[0][1]].values.var() == 0:
        pass
      else:
        """Pearson correlation"""
        cor, p = stats.pearsonr(df[subspaces[0][0]].values, df[subspaces[0][1]].values)
        if abs(cor) > conf.pearson_threshold:
          #print cor
          selection.append(subspaces[0])
      tested.append(subspaces[0])
      subspaces.pop(0)
    else:
      subspaces.pop(0)

  return selection

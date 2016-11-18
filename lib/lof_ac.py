#!/usr/bin/python

#Description:
#---------------------
#project configuration file
#---------------------
#Updated by coder-james

import numpy as np
import scipy.stats as stats
from scipy.spatial.distance import pdist, squareform
from time import time
from sklearn.neighbors import NearestNeighbors

class LOF():
    def __init__(self, k, include=True, log=True):
	self.include = include
	self.k = k
        self.log = log
  
    def skl_kneighbors(self, data):
        add = 1 if not self.include else 0
        nbrs = NearestNeighbors(n_neighbors=self.k + add, algorithm="ball_tree").fit(data)
        distance,indices = nbrs.kneighbors(data)
        return distance[:,add:],indices[:,add:]

    def sci_kneighbors(self, data):
        distance = squareform(pdist(data))
        indices = stats.mstats.rankdata(distance, axis=1)
        if self.include:
	  indices_k = indices <= self.k
	else:
	  indices_k = ((indices <= self.k + 1) & (indices > 1))
        return distance, indices_k

    def fit(self, data):
        t3 = time()
        #print "scipy k neighbors"
        #distance,indices_k = self.sci_kneighbors(data)
        if self.log: print "sikit-learn k neighbors"
        distance,indices_k = self.skl_kneighbors(data)
        #print distance[0]
        #print indices_k[0]
        # k distance
        t4 = time()
        if self.log: print "--- %.4f s" % (t4 - t3)
        if self.log: print "lof function...k distances"
        kdist = np.zeros(len(data))
        for i in range(data.shape[0]):
            #kneighbours = distance[i, indices_k[i, :]]
            #kdist[i] = kneighbours.max()
            kdist[i] = distance[i][-1]
        t5 = time()
        if self.log: print "--- %.4f s" % (t5 - t4)
        if self.log: print "lof function...local reachability density"
        lrd = np.zeros(len(data))
        for i in range(data.shape[0]):
            #lrd[i] = 1/np.maximum(kdist[indices_k[i, :]], distance[i, indices_k[i, :]]).mean()
            below = np.maximum(kdist[indices_k[i]], distance[i]).mean()
            if below == 0:
              lrd[i] = float("inf")
            else:
              lrd[i] = 1 / below
        # lof
        t6 = time()
        if self.log: print "--- %.4f s" % (t6 - t5)
        if self.log: print "lof function...lof value compute"
        lof = np.zeros(len(data))
        for i in range(data.shape[0]):
            #lof[i] = lrd[indices_k[i, :]].mean()/lrd[i]
            top = lrd[indices_k[i]].mean()
            down = lrd[i]
            if top == float("inf") and down == float("inf"):
              lof[i] = 1
            else:
              lof[i] = top / down
            #print data[i],lof[i]
        if self.log: print "--- %.4f s" % (time() - t6) 
	return lof

if __name__ == "__main__":
    x = np.vstack((np.random.random((400, 2)), 100*np.random.random((3, 2))))
    m = LOF(10)
    lofs = m.fit(x)
    print lofs
    print "%.3f" %lofs[0]

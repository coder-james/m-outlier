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
    def __init__(self, k, include=True):
	self.include = include
	self.k = k
  
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
        print "sikit-learn k neighbors"
        distance,indices_k = self.skl_kneighbors(data)
        print distance[0]
        print indices_k[0]
        # k distance
        t4 = time()
        print "--- %.4f s" % (t4 - t3)
        print "lof function...k distances"
        kdist = np.zeros(len(data))
        for i in range(data.shape[0]):
            #kneighbours = distance[i, indices_k[i, :]]
            #kdist[i] = kneighbours.max()
            kdist[i] = distance[i][-1]
        t5 = time()
        print "--- %.4f s" % (t5 - t4)
        print "lof function...local reachability density"
        lrd = np.zeros(len(data))
        for i in range(data.shape[0]):
            #lrd[i] = 1/np.maximum(kdist[indices_k[i, :]], distance[i, indices_k[i, :]]).mean()
            lrd[i] = 1/np.maximum(kdist[indices_k[i]], distance[i]).mean()
        # lof
        t6 = time()
        print "--- %.4f s" % (t6 - t5)
        print "lof function...lof value compute"
        lof = np.zeros(len(data))
        for i in range(data.shape[0]):
            #lof[i] = lrd[indices_k[i, :]].mean()/lrd[i]
            lof[i] = lrd[indices_k[i]].mean()/lrd[i]
            #print data[i],lof[i]
        print "--- %.4f s" % (time() - t6) 
	return lof

if __name__ == "__main__":
    x = np.vstack((np.random.random((400, 2)), 100*np.random.random((3, 2))))
    m = LOF(10)
    lofs = m.fit(x)
    print lofs
    print "%.3f" %lofs[0]

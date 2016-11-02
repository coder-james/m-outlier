#!/usr/bin/python

#Description:
#---------------------
#Test Scripts for KMeans
#---------------------
#Create by coder-james

from sklearn.cluster import KMeans
import numpy as np

def sample():
  X = np.array([[1,2],[1,4],[1,0],
	      [4,2],[4,4],[4,0]])
  kmeans = KMeans(n_clusters=2, random_state=0).fit(X)
  print kmeans.labels_
  assert kmeans.predict([[0,0]])[0] == 0
  assert kmeans.predict([[4,4]])[0] == 1
  print kmeans.cluster_centers_

if __name__ == "__main__":
  sample()

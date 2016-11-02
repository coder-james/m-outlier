#!/usr/bin/python
# -*- coding: utf8 -*-

#Description:
#---------------------
#project configuration file
#---------------------
#Updated by coder-james

def distance_euclidean(instance1, instance2):
    differences = [attr1 - attr2 for _, (attr1, attr2) in enumerate(zip(instance1, instance2))]
    mse = (sum(map(lambda x: x**2, differences)) / len(differences))**0.5
    return mse

class LOF:
    def __init__(self, instances, distance_function=distance_euclidean):
        self.instances = instances
        self.distance_function = distance_function
        min_values = [float("inf")] * len(self.instances[0])
        max_values = [float("-inf")] * len(self.instances[0])
        for instance in self.instances:
            min_values = tuple(map(lambda x,y: min(x,y), min_values,instance)) 
            max_values = tuple(map(lambda x,y: max(x,y), max_values,instance))

        diff = [dim_max - dim_min for dim_max, dim_min in zip(max_values, min_values)]
        if not all(diff):
            problematic_dimensions = ", ".join(str(i+1) for i, v in enumerate(diff) if v == 0)

        self.max_attribute_values = max_values
        self.min_attribute_values = min_values

    def local_outlier_factor(self, min_pts, instance):
        (k_distance_value, neighbours) = self.k_distance(min_pts, instance, self.instances)
        instance_lrd = self.local_reachability_density(min_pts, instance, self.instances)
        lrd_ratios_array = [0]* len(neighbours)
        for i, neighbour in enumerate(neighbours):
            instances_without_instance = set(self.instances)
            instances_without_instance.discard(neighbour)
            neighbour_lrd = self.local_reachability_density(min_pts, neighbour, instances_without_instance)
            lrd_ratios_array[i] = neighbour_lrd / instance_lrd
        return sum(lrd_ratios_array) / len(neighbours)

    def k_distance(self, k, instance, instances):
        distances = {}
        for instance2 in instances:
            distance_value = self.distance_function(instance, instance2)
            if distance_value in distances:
                distances[distance_value].append(instance2)
            else:
                distances[distance_value] = [instance2]
        distances = sorted(distances.items())
        neighbours = []
        [neighbours.extend(n[1]) for n in distances[:k]]
        k_distance_value = distances[k - 1][0] if len(distances) >= k else distances[-1][0]
        return k_distance_value, neighbours
    
    def reachability_distance(self, k, instance1, instance2,instances):
        (k_distance_value, neighbours) = self.k_distance(k, instance2, instances)
        return max([k_distance_value, self.distance_function(instance1, instance2)])
    
    def local_reachability_density(self, min_pts, instance, instances):
        (k_distance_value, neighbours) = self.k_distance(min_pts, instance, instances)
        reachability_distances_array = [0]*len(neighbours)
        for i, neighbour in enumerate(neighbours):
            reachability_distances_array[i] = self.reachability_distance(min_pts, instance, neighbour, instances)
        if not any(reachability_distances_array):
            return float("inf")
        else:
            return len(neighbours) / sum(reachability_distances_array)
    
def outliers(k, instances):
    instances_value_backup = instances
    outliers = []
    for i, instance in enumerate(instances_value_backup):
        instances = list(instances_value_backup)
        instances.remove(instance)
        l = LOF(instances)
        value = l.local_outlier_factor(k, instance)
        #if value > 1:
        #    outliers.append({"lof": value, "instance": instance, "index": i})
        outliers.append([instance, value])
    #outliers.sort(key=lambda o: o["lof"], reverse=True)
    return outliers

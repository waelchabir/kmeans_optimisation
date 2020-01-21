# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf
from math import sqrt
import time

MAX_PARTITIONS_COUNT = int(20)

def computeDistance(x,y):
    return sqrt(sum([(a - b)**2 for a,b in zip(x,y)]))

def closestClusterTuple(dist_list):
    cluster = dist_list[0]
    min_dist = dist_list[1]
    for i in range(2, len(dist_list), 2):
        if dist_list[i+1]<min_dist:
            cluster = dist_list[i]
            min_dist = dist_list[i+1]
    return (cluster, min_dist)

def sumList(x,y):
    return [x[i]+y[i] for i in range(len(x))]

def moyenneList(x,n):
    print('x: ' + str(x))
    print('n: ' + str(n))
    return [x[i]/n for i in range(len(x))]

def simpleKmeans(data, nb_clusters):
    logs = []
    clusteringDone = False
    number_of_steps = 0
    current_error = float("inf")
    # A broadcast value is sent to and saved  by each executor for further use
    # instead of being sent to each executor when needed.
    nb_elem = sc.broadcast(data.count())

    #############################
    # Select initial centroides #
    #############################

    centroides = sc.parallelize(data.takeSample('withoutReplacment',1)).zipWithIndex().map(lambda x: (x[1],x[0][1][:-1]))
    centroid_dict = centroides.collectAsMap()
    centroides.persist()
    for i in range(1, nb_clusters):
        L = [index for index in range(i)]
        temp = data.flatMap(lambda x :((j, x) for j in L ))
        temp1 = temp.map(lambda x : (x[1][0] , (x[0], computeDistance(x[1][1][:-1] , centroid_dict[x[0]] )), x[1][1][:-1]))
        newCentroid = temp1.max(lambda x:x[1][1])
        centroides = centroides.union(sc.parallelize([(i,newCentroid[2])]))
        centroid_dict = centroides.collectAsMap()
    # (0, [4.4, 3.0, 1.3, 0.2])
    # In the same manner, zipWithIndex gives an id to each cluster

    while not clusteringDone:

        #############################
        # Assign points to clusters #
        #############################

        L = [i for i in range(nb_clusters)]
        dist = data.flatMap(lambda x :((i, x) for i in L )) \
            .map(lambda x : (x[1][0] , (x[0], computeDistance(x[1][1][:-1] , centroid_dict[x[0]] ))))
        # (0, (0, 0.866025403784438))

        dist_list = dist.reduceByKey(lambda accum, v: accum + v)
        # [(0, (2, 0.8062257748298554, 1, 5.047771785649585, 0, 0.14142135623730995)), ...]

        # We keep only the closest cluster to each point.
        min_dist = dist_list.mapValues(closestClusterTuple)
        # (0, (2, 0.5385164807134504))

        # assignment will be our return value : It contains the datapoint,
        # the id of the closest cluster and the distance of the point to the centroid
        assignment = min_dist.join(data)

        # (0, ((2, 0.5385164807134504), [5.1, 3.5, 1.4, 0.2, 'Iris-setosa']))

        ############################################
        # Compute the new centroid of each cluster #
        ############################################
        
        clusters = assignment.map(lambda x: (x[1][0][0], (x[1][1][:-1] + [1])))
        # (2, ([5.1, 3.5, 1.4, 0.2, 1]))
        sommes = clusters.reduceByKey(sumList)
        centroidesCluster = sommes.map(lambda x: (x[0],moyenneList(x[1][:-1],x[1][len(x[1])-1]) ))

        ############################
        # Is the clustering over ? #
        ############################

        # Let's see how many points have switched clusters.
        if number_of_steps > 0:
            switch = prev_assignment.join(min_dist)\
                                    .filter(lambda x: x[1][0][0] != x[1][1][0])\
                                    .count()
        else:
            switch = 150
        if switch == 0 or number_of_steps == 100:
            clusteringDone = True
            error = sqrt(min_dist.map(lambda x: x[1][1]).reduce(lambda x,y: x + y))/nb_elem.value
        else:
            centroides = centroidesCluster
            centroides.persist()
            prev_assignment = min_dist
            number_of_steps += 1

    return (assignment, error, number_of_steps, logs)


if __name__ == "__main__": 

    conf = SparkConf().setAppName('exercice')
    sc = SparkContext(conf=conf)

    lines = sc.textFile("hdfs:/user/user84/kMeans/data/iris.data.txt")
    print('*********** num partitions: ' + str(lines.getNumPartitions()))
    data = lines.map(lambda x: x.split(','))\
            .map(lambda x: [float(i) for i in x[:4]]+[x[4]])\
            .zipWithIndex()\
            .map(lambda x: (x[1],x[0]))
    # zipWithIndex allows us to give a specific index to each point
    # (0, [5.1, 3.5, 1.4, 0.2, 'Iris-setosa'])

    print("*************************** Starting Kmeans  ***************************")
    print('Data count = ' + str(data.count()))
    start_time = time.time()
    clustering = simpleKmeans(data,3)
    print(time.time() - start_time)
    print("*************************** Finish Kmeans  ***************************")
    print('error: ' + str(clustering[1]) + ' number_of_steps: ' + str(clustering[2]))
    print('logs: \n')
    for el in clustering[3]:
        print(el)


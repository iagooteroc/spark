#!/usr/bin/python
# coding=utf-8

import numpy as np
from pyspark import SparkContext, SparkConf

def intToBase(n):
        if n==1: return "A"
        if n==2: return "C"
        if n==3: return "G"
        return "T"

def generateString(length):
        ints=np.random.random_integers(1,4,length)
        result="".join(list(map(lambda x: intToBase(x),ints)))
        return result
        
if __name__ == "__main__":
	conf = SparkConf().setAppName("ACGTGenerator").setMaster("local[8]")
	sc=SparkContext.getOrCreate(conf=conf)
	
	dataset=sc.parallelize(range(40)).map(lambda x:generateString(1000))
	dataset.saveAsTextFile("dataset.txt")

	sc.stop()

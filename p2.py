#!/usr/bin/python3
from pyspark import SparkContext, SparkConf
from Bio import pairwise2
# Para correr:
# spark-submit p2.py 2>&1 | grep -v INFO

conf = SparkConf().setAppName("ej1").setMaster("local[4]")
sc = SparkContext(conf=conf)

working_dir = 'material_spark/'
dataset_dir = working_dir + 'dataset.txt'
cadena_dir = working_dir + 'cadena.txt'

rddCadenas = sc.textFile(dataset_dir) # Lee el fichero de texto y crea un elemento en el RDD por cada línea


cadena_f = open(cadena_dir, "r")
cadena = cadena_f.read()
cadena_f.close()

# rddAlineamientos = rddCadenas.map(lambda c: fitness_align(cadena,c))
rddAlineamientos = rddCadenas.map(lambda c: pairwise2.align.globalms(cadena, c, 1, -1, -1, -1, penalize_end_gaps=(True, False))[0])
# print(rddAlineamientos.first())
print('Mayor puntuación:')
print(rddAlineamientos.max(lambda x: x[2]))
print('===================================')
print('Menor puntuación:')
print(rddAlineamientos.max(lambda x: -x[2]))


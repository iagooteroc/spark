#!/usr/bin/python3
from pyspark import SparkContext, SparkConf
from Bio import pairwise2
import time
# Para correr:
# spark-submit p2.py 2>&1 | grep -v INFO
n_proc = "local[2]"
conf = SparkConf().setAppName("p2").setMaster(n_proc)
sc = SparkContext(conf=conf)

working_dir = 'material_spark/'
dataset_dir = working_dir + 'dataset.txt'
cadena_dir = working_dir + 'cadena.txt'

rddCadenas = sc.textFile(dataset_dir) # Lee el fichero de texto y crea un elemento en el RDD por cada línea


cadena_f = open(cadena_dir, "r")
cadena = cadena_f.read()
cadena_f.close()

start_time = time.time()
rddAlineamientos = rddCadenas.map(lambda c: pairwise2.align.globalms(cadena, c, 1, -1, -1, -1, penalize_end_gaps=(True, False))[0]).map(lambda x: (x[2],x[0],x[1]))
best_al = rddAlineamientos.max(lambda x: x[0])
end_time1 = time.time()
worst_al = rddAlineamientos.max(lambda x: -x[0])
end_time2 = time.time()
print('###################################')
print('n_proc:',n_proc)
print('===================================')
print('Mayor puntuación:')
print(best_al)
print('===================================')
print('Time:', end_time1-start_time)
print('===================================')
print('Menor puntuación:')
print(worst_al)
print('===================================')
print('Time:', end_time2-start_time)
print('###################################')


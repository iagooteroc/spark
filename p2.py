#!/usr/bin/python3
from pyspark import SparkContext, SparkConf
import fitting_alignment

n_proc = "local[8]"
conf = SparkConf().setAppName("p2").setMaster(n_proc)
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")

working_dir = ''
dataset_dir = working_dir + 'dataset.txt'
cadena_dir = working_dir + 'cadena.txt'

# Lee el fichero de texto y crea un elemento en el RDD por cada línea
rddCadenas = sc.textFile(dataset_dir) 
# Leemos la cadena de referencia
cadena_f = open(cadena_dir, "r")
cadena = cadena_f.read()
# Eliminamos el salto de línea (\n) al final de la cadena
cadena = cadena[:-1]
cadena_f.close()

# Aplicamos la función de fitting (eliminando el último caracter de c porque es un '\n')
rddAlineamientos = rddCadenas.map(lambda c: fitting_alignment.alinea(c[:-1],cadena))#.cache()
best_al = rddAlineamientos.max(lambda x: x[0])
worst_al = rddAlineamientos.min(lambda x: x[0])
print('###################################')
print('n_proc:',n_proc)
print('===================================')
print('Mayor puntuación:')
print(best_al)
print('===================================')
print('Menor puntuación:')
print(worst_al)
print('===================================')
input("Press Enter to finish...")
print('###################################')

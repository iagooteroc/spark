#!/usr/bin/python3
from pyspark import SparkContext, SparkConf
from Bio import pairwise2
import re
import fitting_alignment

# c++ -O3 -Wall -shared -std=c++11 -fPIC `python3 -m pybind11 --includes` fitting_alignment.cpp -o fitting_alignment`python3-config --extension-suffix`

n_proc = "local[2]"
conf = SparkConf().setAppName("p2").setMaster(n_proc)
sc = SparkContext(conf=conf)

working_dir = ''
dataset_dir = working_dir + 'dataset.txt'
cadena_dir = working_dir + 'cadena.txt'

""" Fitting Alignment """
def fitting(seq1,seq2):
    align = pairwise2.align.globalms(seq1, seq2, 1, -1, -1, -1, penalize_end_gaps=(True, False))

    # Buscamos el comienzo y final del alineamiento
    # para obtener la misma sección en la secuencia de referencia
    start = re.search(r'[^\-]', align[0][1]).start()
    end = re.search(r'[^\-]', align[0][1][::-1]).start()
    end = len(align[0][1]) - end

    # Devolvemos, del mejor alineamiento (align[0]), el score y las cadenas modificadas
    return (align[0][2], align[0][0][start:end], align[0][1][start:end])

# Lee el fichero de texto y crea un elemento en el RDD por cada línea
rddCadenas = sc.textFile(dataset_dir) 

# Leemos la cadena de referencia
cadena_f = open(cadena_dir, "r")
cadena = cadena_f.read()
# Eliminamos el salto de línea (\n) al final de la cadena
cadena = cadena[:-1]
cadena_f.close()

# Aplicamos la función de fitting (eliminando el último caracter de c porque es un '\n')
rddAlineamientos = rddCadenas.map(lambda c: fitting_alignment.alinea(c[:-1],cadena)).cache()
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

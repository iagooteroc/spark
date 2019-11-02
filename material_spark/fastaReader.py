from pyspark import SparkContext, SparkConf
from Bio import pairwise2

# Recorre la lista de índices de las cabeceras (elements)
# y se queda con la mayor de las que son menor a query
def findLastSmallerEqual(query, elements):
        previous=None
        for e in elements:
                if e>query:
                        return previous
                previous=e
        return -1
        
# Convierte la lista de pares línea/índice en un sólo par:
# la cabecera y las líneas de la secuencia concatenadas.
def listToSequenceTuple(groupElements):
        header=None
        seqParts=[]
        # recorre las líneas, separando la cabecera
        # y añadiendo las demás a seqParts
        for seqPart,lineNumber in groupElements:
                if seqPart[0]=='>':
                        header=seqPart
                else:
                        seqParts.append((lineNumber,seqPart))
        seqParts.sort(key=lambda lineNumber_seqPart: lineNumber_seqPart[0])
        # concatena las parte de las líneas que corresponde a la secuencia
        sequence="".join(map(lambda lineNumber_seqPart: lineNumber_seqPart[1], seqParts))
        return (header,sequence)

if __name__ == "__main__":
        cadena_dir = 'cadena.txt'
        cadena_f = open(cadena_dir, "r")
        cadena = cadena_f.read()
        cadena_f.close()
        conf = SparkConf().setAppName("FastaReader").setMaster("local[8]")
        sc=SparkContext.getOrCreate(conf=conf)

        """
        zipWithIndex():
        Zips this RDD with its element indices. 
        The ordering is first based on the partition index 
        and then the ordering of items within each partition. 
        So the first item in the first partition gets index 0, 
        and the last item in the last partition receives the largest index. 
        """
        seqsRDD=sc.textFile('schizophrenia.fasta').zipWithIndex()
        # filtra las cabeceras y se queda con su nº de línea
        numerosCabeceras=seqsRDD \
                                .filter(lambda line_lineNumber:len(line_lineNumber[0])>0 and line_lineNumber[0][0]=='>') \
                                .map(lambda line_lineNumber:line_lineNumber[1]) \
                                .collect()
        numerosCabeceras.sort()
        # groupedRDD se queda con la forma: (index,(line, lineNumber))
        #   - index: nº de línea donde empieza la secuencia
        #   - line: una de las líneas de la secuencia
        #   - lineNumber: nº de línea de esta parte de la secuencia
        # aplica la función findLastSmallerEqual al número de línea con los nº de línea de las cabeceras
        # lo que devuelve el nº de línea en el que empieza la secuencia
        # agrupa las secuencias por el nº de línea en el que empiezan
        groupedRDD=seqsRDD \
                        .filter(lambda line_lineNumber:len(line_lineNumber[0])>0) \
                        .map(lambda line_lineNumber: (findLastSmallerEqual(line_lineNumber[1],numerosCabeceras),line_lineNumber)) \
                        .groupByKey()
        # a cada grupo (cada cadena) aplica la función listToSequenceTuple, 
        # que devuelve una tupla con la cabecera y la secuencia
        sequences=groupedRDD.map(lambda groupId_groupElements: listToSequenceTuple(groupId_groupElements[1]))
        #print(sequences.first())
        #print(sequences.count())
        rddAlineamientos = sequences.map(lambda c: (c[0],pairwise2.align.globalms(cadena, c[1], 1, -1, -1, -1, penalize_end_gaps=(True, False))[0])).map(lambda x: (x[1][2],x[1][0],x[1][1],x[0])).cache()
        best_al = rddAlineamientos.max(lambda x: x[0])
        worst_al = rddAlineamientos.min(lambda x: x[0])
        print('###################################')
        print('Mayor puntuación:')
        print(best_al)
        print('===================================')
        print('Menor puntuación:')
        print(worst_al)
        print('###################################')




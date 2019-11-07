
#include <iostream>
#include <iomanip>
#include <string>
#include <bitset>
#include <stdio.h>
#include <cstring>
#include <bitset>
#include <string>
#include <tuple>
#include <pybind11/pybind11.h>

using namespace std;

tuple<string,string,int> alinea(char referencia[], char subcadena[])
{
	size_t largo1 {strlen(referencia)};
	size_t largo2 {strlen(subcadena)};
	largo1++;
	largo2++;
	char cad1[largo1 + 1] {" "}; 
	char cad2[largo2 + 1] {" "};
	strcat(cad1,referencia);
	strcat(cad2,subcadena);
	// hay que tener en cuenta si usas cadenas de c que tienen un caracter al final
	int **matriz = new int*[largo2];
	bitset<3> **matriz_camino = new bitset<3>*[largo2];
	int **new_ptr = matriz + 1;
	bitset<3> **new_ptr2 = matriz_camino + 1;
	bitset<3> camino(2);
	for (size_t i {0}; i < largo2; ++i)
	{
		matriz[i] = new int[largo1];
		matriz_camino[i] = new bitset<3>[largo1];
	}
	int *ptr;
	ptr = *matriz;
	for(size_t i {0}; i < largo1; ++i)
	{
		*ptr = 0;
		++ptr;
	}
	for(size_t i {1}; i < largo2; ++i)
		{
			**new_ptr = -i;
			++new_ptr;
			**new_ptr2 = camino;
			++new_ptr2;

		}

	int score {};
	int *actual {nullptr};
	bitset<3> *actual_camino {nullptr};
	int *izquierda {nullptr};
	int *arriba {nullptr};
	int *diagonal {nullptr};
	char *letra1 {nullptr};
	char *letra2 {nullptr};
  int scores[3] {};
// en la matriz que marca el camino, el primer valor es en diagonal, el segundo hacia arriba, el tercero hacia la izquierda.
	letra2 = cad2 + 1;
	for(size_t i {1}; i < largo2 - 1; ++i)
		{
			izquierda = matriz[i];
			actual = izquierda + 1;
			actual_camino = matriz_camino[i] + 1;
			diagonal = matriz[i - 1];
			arriba = diagonal + 1;
			letra1 = cad1 + 1;
			for(size_t j {1}; j < largo1; ++j)
				{
					if (*letra1 == *letra2)
						score = *diagonal + 1;
					else
						score = *diagonal - 1;
					scores[0] = score;
					scores[1] = *arriba - 1;
					scores[2] = *izquierda - 1;
					camino.reset();
					if (scores[1] > score)
						score = scores[1];
					if (scores[2] > score)
						score = scores[2];
					for (size_t k {0}; k < 3; k++)
						{
							if (scores[k] == score)
								camino[k] = 1;
						}
					*actual_camino = camino;
					*actual = score;
					++izquierda;
					++actual;
					++actual_camino;
					++diagonal;
					++arriba;
					++letra1;
					
				}
			++letra2;
		}
	//la ultima fila por separado para quedarnos con un punto final y no tener que volver a recorrerla
	int max_score {-(static_cast<int>(largo2) - 1)};
	size_t max_index {};
	izquierda = matriz[largo2 - 1];
	actual = izquierda + 1;
	actual_camino = matriz_camino[largo2 - 1] + 1;
	diagonal = matriz[largo2 - 2];
	arriba = diagonal + 1;
	letra1 = cad1 + 1;
	for(size_t j {1}; j < largo1; ++j)
		{
			if (*letra1 == *letra2)
				score = *diagonal + 1;
			else
				score = *diagonal - 1;
			scores[0] = score;
			scores[1] = *arriba - 1;
			scores[2] = *izquierda - 1;
			camino.reset();
			if (scores[1] > score)
				score = scores[1];
			if (scores[2] > score)
				score = scores[2];
			for (size_t k {0}; k < 3; k++)
				{
					if (scores[k] == score)
						camino[k] = 1;
				}
			*actual_camino = camino;
			*actual = score;

			if (score > max_score)
				{
					max_score = score;
					max_index = j;
				}
			++izquierda;
			++actual;
			++actual_camino;
			++diagonal;
			++arriba;
			++letra1;
		}
	size_t i {largo2 - 1};
	size_t j {max_index};
	char *ptr_cad1 {&(cad1[max_index])};
	char *ptr_cad2 {&(cad2[largo2-1])};
	string resultado1 {};
	string resultado2 {};


	while(i != 0)
		{
			camino = matriz_camino[i][j];
		if (camino[0])
			{
				
				resultado1.push_back(*ptr_cad1);
				resultado2.push_back(*ptr_cad2);
				ptr_cad1--;
				ptr_cad2--;
				i--;
				j--;
				
			}
		else if(camino[1])
			{
				resultado1.push_back('-');
				resultado2.push_back(*ptr_cad2);
				ptr_cad2--;
				i--;
			}
		else
			{
				resultado1.push_back(*ptr_cad1);
				resultado2.push_back('-');
				ptr_cad1--;
				j--;
			}
		}
	size_t largo_resultado {resultado1.size()};
	char resultado1_reverse [largo_resultado + 1];
	char resultado2_reverse [largo_resultado + 1];
	j = largo_resultado - 1;
	for (size_t i = 0; i < largo_resultado; i++)
	{
		resultado1_reverse[i] = resultado1[j];
		resultado2_reverse[i] = resultado2[j];
		j--;
	}
	resultado1_reverse[largo_resultado] = '\0';
	resultado2_reverse[largo_resultado] = '\0';
	tuple<string,string,int> result {resultado1_reverse,resultado2_reverse,max_score};

	for (size_t i = 0; i < largo2; ++i)
		{
			delete [] matriz[i];
			delete [] matriz_camino[i];
		}
	delete [] matriz;
	delete [] matriz_camino;
	return result;
}

PYBIND11_MODULE(fitting_alignment, m){
m.def("alinea", &alinea , "Implementación para realizar el fitting alineament basada en el algoritmo de Needleman-Wunsch con variaciones propias de este problema.");
};

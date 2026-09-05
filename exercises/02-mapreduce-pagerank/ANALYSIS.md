# Análisis de PageRank con MapReduce

## Resumen de resultados

La implementación conserva el grafo, redistribuye la masa de los nodos
colgantes y converge usando la norma L1. La ejecución sobre el dataset oficial
`web_graph_large.txt`, con `d = 0.85`, `epsilon = 10⁻⁶` y `max_iter = 50`,
produjo:

| Métrica | Resultado |
|---|---:|
| Nodos (`N`) | 10.000 |
| Aristas (`E`) | 63.195 |
| Nodos colgantes | 300 |
| Iteraciones ejecutadas | 16 |
| Diferencia L1 final | `9.751 × 10⁻⁷` |
| Suma final de ranks | `1.000000000000` |
| Pares por iteración | 73.195 |
| Pares acumulados | 1.171.120 |
| Tiempo observado | 0,445 s |
| Correlación PageRank/in-degree | 0,8655 |

El tiempo es una medición local y depende del equipo. Las cantidades de nodos,
aristas y pares sí son propiedades reproducibles del dataset y del diseño.

## 1. Correctitud

### Preservación del grafo

El mapper emite un mensaje `STRUCT` por nodo y un mensaje `RANK` por arista.
El reducer devuelve `(node, new_rank, neighbors)`, por lo que la salida conserva
la misma forma que necesita la siguiente iteración. Si se eliminara `STRUCT`,
la adyacencia desaparecería después de la primera pasada y PageRank no podría
volver a distribuir los ranks.

### Conservación de la masa

Sea `M` la suma de los ranks de los nodos colgantes. Cada nodo recibe `M/N`, y
la actualización es:

```text
new_rank(P) = (1-d)/N + d * (incoming(P) + M/N)
```

Al sumar sobre todos los nodos, las contribuciones de los nodos no colgantes
aparecen exactamente una vez y la masa `M` se reincorpora completa:

```text
Σ new_rank = (1-d) + d * (Σ rank no colgante + M)
           = (1-d) + d * 1
           = 1
```

Las pruebas confirman esta invariante durante 30 iteraciones. También verifican
una cadena calculable manualmente, la simetría de un ciclo y la solución
estacionaria de un grafo con un nodo colgante.

### Convergencia

La comprobación se realiza fuera de `mapreduce()` mediante:

```text
Σ |new_rank(P) - old_rank(P)| < epsilon
```

El grafo grande convergió en 16 iteraciones con `epsilon = 10⁻⁶`. La cifra de
aproximadamente 24 iteraciones dada en el enunciado es una referencia de escala;
el resultado ejecutado cumple directamente el criterio L1 solicitado.

## 2. Pares emitidos y volumen de shuffle

Para cada nodo se emite un mensaje `STRUCT`. Para cada arista se emite una
contribución `RANK`. Por tanto, en una iteración el mapper produce:

```text
N mensajes STRUCT + E mensajes RANK = N + E pares
```

El número de registros movidos por el shuffle es `N + E`, por lo que su costo
asintótico es `O(N + E)` por iteración. En bytes, los mensajes `STRUCT`
transportan en conjunto las `E` referencias de adyacencia y los mensajes `RANK`
transportan `E` valores numéricos; el volumen sigue siendo `O(N + E)`, pero la
serialización de identificadores, etiquetas y estructuras aumenta la constante.

Para el dataset grande:

```text
N + E = 10.000 + 63.195 = 73.195 pares por iteración
```

En las 16 iteraciones observadas:

```text
73.195 × 16 = 1.171.120 pares
```

Si se forzaran 30 iteraciones:

```text
73.195 × 30 = 2.195.850 pares
```

## 3. Uso de un combiner

El combiner se ubicaría después del Map y antes del Shuffle. Preagregaría, dentro
de cada partición de mapper, las contribuciones `RANK` destinadas a la misma
página:

```text
(P, (RANK, 0.001))
(P, (RANK, 0.003))  ->  (P, (RANK, 0.006))
(P, (RANK, 0.002))
```

La suma es asociativa y conmutativa, por lo que la preagregación no cambia el
resultado. Sí reduce el número de valores que cruzan la red cuando una partición
contiene varias contribuciones dirigidas al mismo destino.

Los mensajes `STRUCT` no deben mezclarse ni sumarse: cada uno representa la
adyacencia que tiene que llegar intacta al reducer. El framework educativo no
implementa combiners ni particiones de mapper, de modo que esta optimización es
una propuesta para Hadoop o un sistema equivalente.

## 4. Data skew

El shuffle garantiza que todas las contribuciones para una página lleguen al
mismo grupo lógico. Si una página tiene in-degree `k`, su grupo contiene `k`
mensajes `RANK` y un mensaje `STRUCT`. Una página con millones de enlaces
entrantes concentra el trabajo en el reducer responsable de esa clave, aunque
otros reducers hayan terminado.

En el dataset oficial, el grupo más grande observado fue el de `P06485`, con 60
contribuciones y un mensaje estructural: 61 mensajes. Ese reducer es el principal
candidato a cuello de botella. Un combiner disminuye repeticiones locales, pero
no elimina la obligación de reunir el total final de la página. Para un skew
extremo sería necesaria una agregación jerárquica o dividir temporalmente la
clave y realizar una segunda suma.

## 5. Conexión con la Clase 5 y Spark

El mapper utiliza `yield`, pero eso no convierte al framework completo en un
pipeline lazy. `mapreduce_framework.py` consume cada generador mediante
`mapped.extend(mapper(item))` y mantiene en memoria las estructuras `mapped`,
`shuffled` y `reduced`. Además, `load_graph()` carga el grafo completo. Por ello,
esta implementación local tiene memoria `O(N + E)` y no es adecuada para un
archivo que exceda la memoria disponible.

En este simulador el archivo se lee una sola vez y el estado se recorre en
memoria en cada vuelta. En un despliegue clásico de Hadoop, PageRank se expresa
como una cadena de jobs: cada iteración lee la salida materializada de la
anterior, ejecuta el shuffle y escribe un nuevo estado. Con 30 iteraciones hay
aproximadamente 30 lecturas completas y 30 escrituras del estado, además del
tráfico del shuffle.

Spark es más apropiado para este patrón porque permite particionar y conservar
el grafo en memoria distribuida entre iteraciones. De este modo reduce la
relectura y reserialización del estado desde disco. Sin embargo, Spark no elimina
el costo de calcular y redistribuir contribuciones en cada iteración, ni resuelve
automáticamente el data skew. Esta diferencia entre reutilizar datos en memoria
y materializar cada pasada es la relación principal con la
[Clase 5: MapReduce en Python](https://app.notion.com/p/3cbe4205157e81469386ed38c1106f13).

Procesar el archivo en lotes simples tampoco es suficiente: contribuciones a una
misma página pueden aparecer en lotes diferentes y la masa colgante es global.
Sería necesario añadir un shuffle externo y una agregación global en disco, lo
que equivale a implementar parte de la infraestructura que ya ofrecen Hadoop,
Spark u otros motores distribuidos.

## 6. Top 15: PageRank frente a in-degree

| Posición | Página | PageRank | In-degree |
|---:|---|---:|---:|
| 1 | P01443 | 0,0011618770 | 57 |
| 2 | P03367 | 0,0010364789 | 59 |
| 3 | P06210 | 0,0009069604 | 32 |
| 4 | P09065 | 0,0008929892 | 55 |
| 5 | P04814 | 0,0008602960 | 60 |
| 6 | P00894 | 0,0008247542 | 55 |
| 7 | P01977 | 0,0008117387 | 49 |
| 8 | P07750 | 0,0008082750 | 44 |
| 9 | P03428 | 0,0007994343 | 15 |
| 10 | P07315 | 0,0007892552 | 55 |
| 11 | P05650 | 0,0007882423 | 22 |
| 12 | P08541 | 0,0007402087 | 29 |
| 13 | P00751 | 0,0007312818 | 20 |
| 14 | P07382 | 0,0007291920 | 48 |
| 15 | P01632 | 0,0007214985 | 41 |

La correlación de Pearson entre PageRank e in-degree fue `0,8655`: es fuerte,
pero no perfecta. El in-degree cuenta todos los enlaces entrantes por igual;
PageRank pondera cada enlace por la importancia de su página de origen y divide
esa importancia entre sus enlaces salientes. Por eso `P03428`, con solo 15
enlaces entrantes, supera a páginas con un in-degree mucho mayor: sus enlaces
pueden proceder de páginas mejor posicionadas o con menor out-degree.

## 7. Reproducción

```bash
python exercises/02-mapreduce-pagerank/pagerank.py
python exercises/02-mapreduce-pagerank/test_pagerank.py
```

La primera orden genera las métricas y el ranking del dataset grande. La segunda
ejecuta los cuatro casos de prueba requeridos.

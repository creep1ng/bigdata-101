# DESIGN.md — PageRank con MapReduce

> Plantilla para la Entrega A. Complétala ANTES de escribir código. Piensa el diseño primero; el código viene después.

**Autor(es): Ricardo Arias** **Fecha: 2026-08-29**

---

## 1. Representación de los datos

¿Cómo representas cada nodo del grafo como un "ítem" que el `mapper` recibe? Describe la estructura exacta (qué es la clave, qué es el valor, qué contiene la adyacencia).

- Key: Nombre del nodo. Por ejemplo:
    - P00107
    - P00027
- Value: Puede ser RANK (el PageRank actual) o NEIGHBORS (los vecinos a los que referencia una página).
    - RANK (`float`):
        - 0.3
        - 0.01
    - NEIGHBORS (`list[str]`):
        - `[P00051, P00027]`
        - `[]`
Un ejemplo de imput puede ser:

```python
row = ("P00027", 0.0124, ("P00027", "P00051"))
#       |        |       |
#       |        |       |- NEIGHBORS
#       |        |
#       |        |- RANK
#       |- Key
```

Se pasan ambos valores RANK y NEIGHBORS a `map()` para preservar la estructura del grafo. Luego `map()` los pasa a `reduce()`, donde la lógica implementada se encargará de actualizar los PageRanks de cada página.

---

## 2. Esquema clave-valor por fase

### MAP — ¿qué emite el mapper por cada nodo?

Especifica **todos** los tipos de mensajes que emites (hay más de uno).

| Tipo de mensaje | Clave | Valor | Propósito |
|-----------------|-------|-------|-----------|
| RANK | Nombre de la página | `float` entre 0 y 1 | Repartir el PageRank de una página entre sus aristas salientes |
| NEIGHBORS | Nombre de la página | `list[str]` | Preservar a lo largo del ciclo los vecinos de cada nodo |

### SHUFFLE — ¿qué queda agrupado por clave?

Por clave queda agrupado el mensaje NEIGHBOR y los mensajes RANK de cada nodo al que es referenciado cada página.

### REDUCE — ¿qué retorna el reducer?

Devuelve la tupla `(key, new_rank, neighbors)`.

---

## 3. Preservación de la estructura del grafo

Explica **cómo** logras que la lista de adyacencia sobreviva de una iteración a la siguiente. ¿Qué pasaría si NO lo hicieras?

Pasando la lista NEIGHBORS como mensaje al `map()`. El `reducer()` calcula la actualización del PageRank de una página concreta y pasa **sin modificar** NEIGHBORS.

En caso de no incluirlo, después de la iteración inicial el bucle no sabría cómo repartir el PageRank de un nodo a sus nodos salientes.
---

## 4. Manejo de dangling nodes

¿Qué haces con el rank de un nodo sin enlaces salientes? ¿Por qué? ¿Cómo afecta esto a la invariante de suma (Σ ranks ≈ 1.0)?

En el bucle externo, antes de realizar el `map()`, sumo el rank de los dangling nodes y divido la suma por el total de elementos. Este sería el aporte relativo de cada dangling node. Luego en el `reduce()` incorporo este valor a la suma de aportes individuales de cada página.

---

## 5. Iteración y convergencia

- ¿Dónde vive el bucle de iteración? (fuera de `mapreduce()`)
    En un archivo `exercises/02-mapreduce-pagerank/pagerank.py` que provee las funciones para serializar el archivo y la función de cálculo de `pagerank()`.
- Criterio de convergencia (norma L1 < epsilon):
    Un valor de tolerancia comentado frecuentemente en internet es de $10^{-6}$.
- ¿Cómo comparas los ranks entre iteración N y N+1?
    Almaceno en dos valores `global_pagerank` y `global_pagerank_new`.

---

## 6. Diagrama de una iteración

![Diagrama de diseño de PageRank sobre MapReduce](diagram.png)


# Bitácora de uso de IA

## Alcance

Se utilizó Codex como asistente para revisar el enunciado, cuestionar decisiones de diseño, implementar el algoritmo, preparar pruebas y estructurar el análisis.
El asistente produjo código y borradores documentales; yo dirigí las preguntas,
evalué las explicaciones y solicité correcciones y entregables faltantes.

Esta bitácora fue estructurada por Codex a partir del historial de trabajo y fue validada por mí (Ricardo) antes de la entrega.

## Interacciones relevantes

| Solicitud o discusión | Aporte de la IA | Limitación detectada y corrección |
|---|---|---|
| Leer completamente el enunciado, la rúbrica y la plantilla antes de implementar. | Identificó que cada iteración debía preservar la adyacencia, redistribuir la masa colgante y comprobar convergencia fuera de `mapreduce()`. | La respuesta inicial era un análisis, no un entregable ejecutable. Primero se completó y discutió `DESIGN.md`, respetando el orden de la tarea. |
| Pregunta sobre si el mapper podía emitir por clave el nodo de origen y un diccionario de contribuciones a sus vecinos. | Explicó que el shuffle agrupa por clave y que las contribuciones deben usar como clave el nodo de destino. | La propuesta evaluada agrupaba por emisor, por lo que un reducer no recibiría todos los aportes de una página. Se adoptaron mensajes `RANK` por destino y un mensaje estructural separado. |
| Implementar `DESIGN.md` en `pagerank.py` y hacer que `main()` generara datos para el análisis. | Generó el parser, mapper, reducer, bucle iterativo, convergencia L1, redistribución de masa colgante y reporte del dataset grande. | La primera entrega era parcial respecto a todos los archivos exigidos por la Entrega B. También apareció una diferencia de nombre entre `NEIGHBORS` en el diseño y `STRUCT` en el código. Se añadieron los archivos faltantes y se alineó el diseño con la implementación final. |
| Implementar los casos de prueba solicitados. | Generó pruebas para una cadena de tres nodos, un ciclo, un nodo colgante y la suma de ranks durante 30 iteraciones. | No bastaba con comprobar solamente que el programa terminara. Se usaron resultados calculados a mano y soluciones estacionarias conocidas para que las pruebas verificaran valores, no solo ejecución. |
| Relacionar PageRank con las notas de la Clase 5, especialmente `yield`, carga eager y Spark. | Diferenció la generación lazy del mapper del comportamiento eager del framework y derivó el costo `N + E`. | La interpretación inicial podía sugerir que usar `yield` evitaba cargar todos los resultados. Al revisar `mapped.extend()`, se corrigió: el generador se consume inmediatamente y el framework acumula Map, Shuffle y Reduce en memoria. |
| Terminar el análisis y los puntos pedidos por el enunciado. | Preparó `ANALYSIS.md`, esta bitácora y la corrección final de `DESIGN.md`. | Las métricas se volvieron a ejecutar sobre `web_graph_large.txt` y se separaron los resultados reproducibles de la medición de tiempo dependiente del equipo. |

## Qué estaba mal o incompleto

No se detectó un error algorítmico en la implementación final, pero sí hubo
propuestas, interpretaciones y entregas intermedias incompletas:

1. Una salida agrupada por el nodo de origen no sirve para PageRank en una sola
   pasada, porque el reducer necesita agrupar por destino.
2. Conservar únicamente las contribuciones pierde la estructura del grafo; fue
   necesario incluir un mensaje `STRUCT` en cada iteración.
3. Tratar `yield` como sinónimo de procesamiento streaming era incorrecto para
   este framework, que consume y almacena todos los pares.
4. La primera implementación no completaba por sí sola `test_pagerank.py`,
   `ANALYSIS.md` y `AI_LOG.md`, que también forman parte de la Entrega B.
5. El diseño y el código usaban nombres diferentes para el mensaje estructural;
   se unificaron bajo `STRUCT`.

## Qué corregí y por qué

- Cuestioné el esquema clave-valor antes de implementar y confirmé que las
  contribuciones debían agruparse por página de destino.
- Solicité explícitamente pruebas que demostraran casos calculables, nodos
  colgantes y conservación de masa, en lugar de aceptar solo una ejecución del
  dataset grande.
- Relacioné la solución con las notas de la Clase 5 y pedí profundizar el costo,
  los límites de memoria y la comparación con Spark.
- Pedí completar el análisis y la bitácora cuando la entrega todavía contenía
  únicamente el diseño, la implementación y las pruebas.
- Conservé el framework original sin modificaciones para que las decisiones de
  iteración, estructura y masa colgante fueran visibles en la solución.

## Verificación realizada

Se ejecutaron los siguientes comandos:

```bash
python exercises/02-mapreduce-pagerank/pagerank.py
python exercises/02-mapreduce-pagerank/test_pagerank.py
```
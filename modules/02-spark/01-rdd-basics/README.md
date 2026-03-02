# RDD Basics: From MapReduce to Spark

## The Bridge

You already know MapReduce — the three phases:

```
Input → MAP → Shuffle/Sort → REDUCE → Output
```

Spark was created to solve MapReduce's limitations while keeping the same idea:

| Aspect | Traditional MapReduce | Spark |
|--------|----------------------|-------|
| Processing | Disk between steps | In-memory |
| Operations | Only map + reduce | map, filter, join, groupBy, ... |
| Workflow | Rigid 2-phase pipeline | Flexible chain of transformations |
| Speed | Slow (disk I/O) | 10-100x faster |

## What is an RDD?

**Resilient Distributed Dataset** — Spark's core abstraction. A distributed collection you transform with operations.

```
Your Python list:     [1, 2, 3, 4, 5]
An RDD:               [1, 2, 3, 4, 5]  ← distributed across machines
```

Two types of operations:
- **Transformations** (lazy — build a plan): `map()`, `filter()`, `flatMap()`, `reduceByKey()`
- **Actions** (trigger execution): `collect()`, `count()`, `take()`, `saveAsTextFile()`

## Side-by-Side: WordCount

### Pure Python (what you know)
```python
def mapper(line):
    for word in line.lower().split():
        yield (word, 1)

def reducer(key, values):
    return sum(values)

results = mapreduce(text, mapper, reducer)
```

### mrjob (what you know)
```python
class MRWordCount(MRJob):
    def mapper(self, _, line):
        for word in re.findall(r'\w+', line.lower()):
            yield (word, 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))
```

### Spark RDD (what you'll learn)
```python
results = (
    sc.parallelize(text)
    .flatMap(lambda line: line.lower().split())
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
    .collect()
)
```

Same logic: split → pairs → sum. But chained, in-memory, and optimized.

## Lazy Evaluation

The biggest conceptual shift. In MapReduce, each step runs immediately. In Spark:

```python
rdd1 = sc.parallelize(data)           # Nothing happens
rdd2 = rdd1.map(lambda x: x * 2)     # Nothing happens
rdd3 = rdd2.filter(lambda x: x > 5)  # Nothing happens
result = rdd3.collect()               # NOW everything executes
```

Spark builds a DAG (execution plan) and optimizes before running.

## Files

- **[`rdd_wordcount.py`](rdd_wordcount.py)** — WordCount with RDDs (bridge from MapReduce)
- **[`rdd_temperature.py`](rdd_temperature.py)** — Average temperature per city
- **[`rdd_sales.py`](rdd_sales.py)** — Sales analysis with RDDs
- **[`rdd_logs.py`](rdd_logs.py)** — Log processing and filtering

### Real-World Exercise
- **[`rdd_uber.py`](rdd_uber.py)** — Uber Movement Travel Times Analysis
  - Processes real data from Uber Movement across 7 cities (Bogota, Boston, Paris, Sydney, Manila, Washington DC, Johannesburg)
  - ~5,500 records of zone-to-zone travel times
  - Analyzes: trips per city, average travel times, longest routes, travel time variability
  - Saves results to disk with timestamped folders
  - Dataset: `/datasets/uber/`

#### About the Dataset

The Uber Movement dataset contains average travel times between geographic zones, derived from GPS pings recorded every 4 seconds by the Uber driver app. The pipeline Uber uses is essentially a MapReduce process (see full methodology in [`datasets/uber/Uber Movement - Travel Times Methodology.pdf`](../../../datasets/uber/Uber%20Movement%20-%20Travel%20Times%20Methodology.pdf)):

1. **Map** — Each GPS ping is assigned to a geographic zone (neighborhood, census tract)
2. **Aggregate** — For each zone a trip crosses, the average timestamp is computed (individual trajectories are lost)
3. **Reduce** — Travel times are aggregated across all trips for each origin-destination pair
4. **Privacy** — Zone pairs with too few trips or unique riders are removed

The CSV columns we use:
- `Origin/Destination Display Name` — Zone names
- `Mean Travel Time (Seconds)` — Average travel time
- `Range - Lower/Upper Bound` — Confidence interval

#### RDD Patterns Used in rdd_uber.py

The Uber exercise combines several RDD patterns in one script. Here's what each analysis does:

**1. Count by key** — Trips per city
```python
# Pattern: map to (key, 1) → reduceByKey to sum
rdd.map(lambda trip: (trip[0], 1))
   .reduceByKey(lambda a, b: a + b)
```

**2. Average by key** — Average travel time per city
```python
# Pattern: map to (key, (value, 1)) → reduceByKey → mapValues to divide
rdd.map(lambda trip: (city, (seconds, 1)))
   .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))  # (total_sec, count)
   .mapValues(lambda v: v[0] / v[1])                    # average
```
This is a common pattern because Spark has no built-in "average by key" for RDDs. You carry the count alongside the sum, then divide at the end.

**3. Top-N** — Longest trips
```python
# sortBy + take(N) instead of collect() to avoid loading everything into memory
rdd.sortBy(lambda trip: trip[3], ascending=False).take(5)
```

**4. Cache** — Reuse the same RDD
```python
all_trips.cache()  # Keep in memory after first computation
```
Without `cache()`, Spark would re-read and re-parse all 7 CSV files for each of the 4 analyses. With `cache()`, it reads once and reuses.

**5. Saving to disk** — `saveAsTextFile`
```python
rdd.coalesce(1).saveAsTextFile(output_path)
```
`coalesce(1)` merges all partitions into a single file. Good for small summaries, but never use it for large datasets — it forces all data through one node.

Each run saves to a timestamped folder (`output/uber_results/20260302_001234/`) so results don't overwrite each other. This is necessary because `saveAsTextFile` fails if the directory already exists.

#### Handling Different CSV Formats

The dataset has two CSV formats:
- **With geometry** (10 columns): Bogota, Boston, Sydney, Manila, Washington DC
- **Without geometry** (8 columns): Paris, Johannesburg

The script handles this by using negative indexing for the last 3 columns (which are always Mean, Lower, Upper regardless of format):
```python
mean_travel_time = int(fields[-3])
lower_bound = int(fields[-2])
upper_bound = int(fields[-1])
```

## Key Concepts

| Concept | Description |
|---------|-------------|
| `SparkSession` | Modern entry point — creates SparkContext internally |
| `SparkContext (sc)` | Creates RDDs and manages cluster connection |
| `flatMap` vs `map` | `map`: one output per input. `flatMap`: zero or more |
| `reduceByKey` | Groups by key and reduces — like shuffle + reduce in one step |
| `collect()` | Brings all results to the driver (triggers execution) |

## Try It

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /app/01-rdd-basics/rdd_wordcount.py
docker compose exec spark-master /opt/spark/bin/spark-submit /app/01-rdd-basics/rdd_temperature.py
docker compose exec spark-master /opt/spark/bin/spark-submit /app/01-rdd-basics/rdd_sales.py
docker compose exec spark-master /opt/spark/bin/spark-submit /app/01-rdd-basics/rdd_logs.py
docker compose exec spark-master /opt/spark/bin/spark-submit /app/01-rdd-basics/rdd_uber.py
```

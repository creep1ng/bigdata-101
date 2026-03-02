# Spark Architecture

## Overview

Understanding Spark's architecture is essential before deploying a cluster. This section covers what happens under the hood when you run a Spark job.

All concepts here are explorable through the Spark UI at http://localhost:4040 (while a job is running).

## Components

```
┌─────────────────────────────────────────────┐
│                CLUSTER MANAGER              │
│         (Standalone / YARN / Mesos)         │
└──────────────────┬──────────────────────────┘
                   │ allocates resources
        ┌──────────┴──────────┐
        ▼                     ▼
┌──────────────┐    ┌──────────────┐
│    DRIVER    │    │   EXECUTOR   │  ×N
│              │    │              │
│ SparkContext │───▶│  Task  Task  │
│ DAG Scheduler│    │  Task  Task  │
│ Task Scheduler│   │              │
│ Spark UI     │    │  Cache/Memory│
└──────────────┘    └──────────────┘
```

### Driver
- Your main program (where `SparkSession` is created)
- Converts your code into a DAG of stages and tasks
- Schedules tasks on executors
- Hosts the Spark UI (port 4040)

In our Docker setup: the `spark-submit` process is the driver.

### Executor
- Worker process that runs tasks
- Stores data in memory/disk (cache)
- One or more per worker node
- Reports status back to driver

In our Docker setup: `spark-worker-1` and `spark-worker-2` run executors.

### Cluster Manager
- Allocates resources (CPU, memory) across the cluster
- Types: Standalone (built-in), YARN (Hadoop), Mesos, Kubernetes

In our Docker setup: `spark-master` is the Standalone cluster manager.

## How a Job Executes

```
Your Code
    │
    ▼
1. DAG Creation
   sc.parallelize(data).map(...).reduceByKey(...).collect()
   becomes a graph of transformations
    │
    ▼
2. Stage Division
   Spark splits the DAG at "shuffle boundaries"
   (reduceByKey, groupBy, join cause shuffles)
    │
    ▼
3. Task Scheduling
   Each stage is divided into tasks (one per partition)
   Tasks are sent to executors
    │
    ▼
4. Execution
   Executors run tasks in parallel
   Results flow back to the driver
```

## Partitions

Data in an RDD is split into partitions — chunks processed in parallel.

```python
rdd = sc.parallelize([1,2,3,4,5,6,7,8], numSlices=4)
# Partition 0: [1, 2]
# Partition 1: [3, 4]
# Partition 2: [5, 6]
# Partition 3: [7, 8]
```

- More partitions = more parallelism (up to available cores)
- Too many partitions = overhead from scheduling
- Rule of thumb: 2-4 partitions per CPU core

You can inspect partitions with `glom()`:
```python
rdd.glom().collect()  # Returns a list of lists, one per partition
```

## Narrow vs Wide Transformations

This is the key distinction for understanding Spark performance:

| Type | What happens | Examples | Shuffle? |
|------|-------------|----------|----------|
| **Narrow** | Each partition is processed independently | `map`, `filter`, `flatMap` | No |
| **Wide** | Data must move between partitions | `reduceByKey`, `groupByKey`, `join` | Yes |

```
Narrow (map):                    Wide (reduceByKey):
Partition 0 → Partition 0       Partition 0 ─┐
Partition 1 → Partition 1       Partition 1 ─┼→ Shuffle → New partitions
Partition 2 → Partition 2       Partition 2 ─┘
(data stays in place)           (data moves across the network)
```

Every wide transformation creates a new **stage** in the DAG.

## Shuffles

A shuffle redistributes data across partitions. It's the most expensive operation because data moves across the network (or between containers in Docker).

Operations that cause shuffles:
- `reduceByKey()`, `groupByKey()`
- `join()`, `cogroup()`
- `repartition()`, `coalesce()`

```
Before reduceByKey:          After reduceByKey:
Partition 0: (a,1)(b,1)     Partition 0: (a,3)
Partition 1: (a,1)(c,1)  →  Partition 1: (b,1)
Partition 2: (a,1)(b,1)     Partition 2: (c,1)
         ↑ shuffle happens here
```

In the Spark UI, you can see shuffle metrics in the Stages tab: "Shuffle Read" and "Shuffle Write" show how much data moved.

## Cache

When you run multiple actions on the same RDD, Spark recomputes it from scratch each time. `cache()` tells Spark to keep the result in memory after the first computation.

```python
rdd = sc.textFile("big_file.csv").map(parse).cache()

rdd.count()    # First action: reads file, parses, stores in memory
rdd.take(10)   # Second action: reads from memory (fast!)
rdd.sum()      # Third action: reads from memory (fast!)
```

Without `cache()`, Spark would read and parse the file 3 times.

You can see cached RDDs in the Spark UI's **Storage** tab.

## Spark UI

When running a job, access at http://localhost:4040

| Tab | What to look for |
|-----|-----------------|
| **Jobs** | One job per action (`collect`, `count`, `take`). Shows overall progress |
| **Stages** | Stages within each job. Look for shuffle read/write metrics |
| **Storage** | Cached RDDs — memory used, fraction cached |
| **Executors** | Resource usage per executor — tasks completed, memory, shuffle |

## Files

- **[`architecture_demo.py`](architecture_demo.py)** — Interactive demo of all architecture concepts
  - 4 sections: Partitions, Stages/Shuffles, Cache, Narrow vs Wide
  - Pauses at the end so you can explore the Spark UI
  - Run: `docker compose exec -it spark-master /opt/spark/bin/spark-submit /app/02-architecture/architecture_demo.py`

## Exercises

1. Run `architecture_demo.py` and open http://localhost:4040
2. In the **Jobs** tab: count how many jobs were created. Why that number?
3. In the **Stages** tab: find a stage with shuffle. How many bytes were shuffled?
4. In the **Storage** tab: find the cached RDD. How much memory does it use?
5. Go back to `01-rdd-basics/rdd_uber.py` and run it. In the Spark UI:
   - How many jobs does it create?
   - Which jobs have shuffles?
   - Can you see the cached RDD?

## Key Takeaways

| Concept | Why It Matters |
|---------|---------------|
| Driver vs Executor | Know where your code runs vs where data is processed |
| Partitions | Control parallelism — too few wastes cores, too many wastes scheduling |
| Narrow vs Wide | Wide = shuffle = slow. Minimize wide transformations |
| Shuffles | The #1 performance bottleneck — always check shuffle metrics in the UI |
| DAG + Stages | Spark optimizes your code before running. Stages split at shuffles |
| Cache | Avoid recomputing the same data. Essential for multi-analysis scripts |
| Spark UI | Your primary tool to understand and debug job execution |

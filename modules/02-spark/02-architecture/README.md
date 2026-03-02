# Spark Architecture

## Overview

Understanding Spark's architecture is essential before deploying a cluster. This section covers what happens under the hood when you run a Spark job.

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

### Executor
- Worker process that runs tasks
- Stores data in memory/disk (cache)
- One or more per worker node
- Reports status back to driver

### Cluster Manager
- Allocates resources (CPU, memory) across the cluster
- Types: Standalone (built-in), YARN (Hadoop), Mesos, Kubernetes

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

## Shuffles

A shuffle redistributes data across partitions. It's the most expensive operation.

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

## Spark UI

When running locally, access at `http://localhost:4040`

Key tabs:
- **Jobs**: Overall job progress
- **Stages**: Breakdown of each stage, shuffle read/write
- **Storage**: Cached RDDs and their memory usage
- **Executors**: Resource usage per executor
- **SQL**: Query plans (for DataFrames/SQL)

## Files

- **[`architecture_demo.py`](architecture_demo.py)** — Script that demonstrates partitions, stages, and the Spark UI
  - Run it and open `http://localhost:4040` to explore
  - Run: `python architecture_demo.py`

## Key Takeaways

| Concept | Why It Matters |
|---------|---------------|
| Driver vs Executor | Know where your code runs vs where data is processed |
| Partitions | Control parallelism and performance |
| Shuffles | The #1 performance bottleneck — minimize them |
| DAG | Spark optimizes your chain of operations before running |
| Spark UI | Your tool to understand and debug job execution |

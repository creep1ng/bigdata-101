# Spark - Distributed Data Processing

## Introduction

Apache Spark is a unified analytics engine that evolved from MapReduce. While MapReduce writes intermediate results to disk between steps, Spark keeps data in memory and supports a rich set of operations beyond just map and reduce.

In this module you'll work on a real Spark cluster from day one using Docker.

## Prerequisites

- Completed `01-mapreduce` module
- Docker and Docker Compose

## Quick Start

```bash
cd modules/02-spark
docker compose up -d
docker compose exec spark-master spark-submit /app/01-rdd-basics/rdd_wordcount.py
```

See [GETTING_STARTED.md](GETTING_STARTED.md) for full setup instructions.

## Module Structure

### 1. RDD Basics — From MapReduce to Spark
- The same problems you solved with MapReduce, now with RDDs
- Transformations vs Actions, lazy evaluation
- Location: `01-rdd-basics/`

### 2. Spark Architecture
- Driver, Executors, Cluster Manager
- Partitions, DAG, shuffles
- Spark UI: understanding job execution
- Location: `02-architecture/`

### 3. Cluster Management
- Scaling workers, resource configuration
- Monitoring and debugging with Spark UI
- Location: `03-cluster-management/`

## Running Any Script

```bash
docker compose exec spark-master spark-submit /app/<section>/<script>.py
```

## Recommended Progression

1. Start with RDD basics to connect MapReduce concepts with Spark
2. Study the architecture to understand what happens under the hood
3. Experiment with cluster management to see scaling in action
4. Continue with `03-databricks` for DataFrames and SparkSQL

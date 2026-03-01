# PySpark - Distributed Data Processing

## Introduction

PySpark is the Python API for Apache Spark, a unified analytics engine for large-scale data processing. Spark evolved beyond MapReduce by introducing in-memory computation, DAG-based execution, and a rich set of high-level APIs.

## Prerequisites

- Completed `01-mapreduce` module (recommended)
- Python 3.8+
- Java 8+ (required by Spark)

## Module Structure

### 1. RDD Basics
- Core abstraction: Resilient Distributed Datasets
- Bridge from MapReduce: `map()`, `reduce()`, `filter()`
- Transformations vs Actions
- Location: `01-rdd-basics/`

### 2. DataFrames
- Structured data processing with the DataFrame API
- Schema inference and definition
- Transformations, aggregations, and joins
- Location: `02-dataframes/`

### 3. Spark SQL
- Querying data with SQL syntax
- Temporary views and catalogs
- Integration with DataFrames
- Location: `03-spark-sql/`

## Recommended Progression

1. Start with RDD basics to connect MapReduce concepts with Spark
2. Move to DataFrames for the modern, optimized API
3. Learn Spark SQL for declarative data analysis

## Requirements

- Python 3.8+
- Java 8+
- PySpark (`pip install pyspark`)

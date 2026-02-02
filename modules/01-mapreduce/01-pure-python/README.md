# MapReduce with Pure Python

## Overview

Learn MapReduce from the ground up with pure Python implementations. This module is organized in three progressive levels, each building on the previous one.

## Learning Path

### Level 1: Basics (`01-basics/`)
**Goal**: Understand core MapReduce concepts with simple examples

Start here to learn the fundamentals:
- [`mapreduce_framework.py`](01-basics/mapreduce_framework.py) - Core MapReduce implementation
- [`wordcount.py`](01-basics/wordcount.py) - Classic WordCount with inline data
- [`examples/sales.py`](01-basics/examples/sales.py) - Business data analysis
- [`examples/logs.py`](01-basics/examples/logs.py) - Log file analysis

**Key Concepts**: Map function, Reduce function, Shuffle/Sort phase

```bash
cd 01-basics
python wordcount.py
python examples/sales.py
python examples/logs.py
```

---

### Level 2: File Processing (`02-file-processing/`)
**Goal**: Process real files and handle multiple data sources

Learn to work with actual files:
- [`wordcount_file.py`](02-file-processing/wordcount_file.py) - Process single text file
- [`wordcount_multifile.py`](02-file-processing/wordcount_multifile.py) - Process multiple files
- [`sample_text.txt`](02-file-processing/sample_text.txt) - Sample data about MapReduce
- [`sample_bigdata.txt`](02-file-processing/sample_bigdata.txt) - Sample data about Big Data

**Key Concepts**: File I/O, batch processing, aggregating multiple sources

```bash
cd 02-file-processing
python wordcount_file.py sample_text.txt
python wordcount_multifile.py .
```

---

### Level 3: Distributed Simulation (`03-distributed-simulation/`)
**Goal**: Understand distributed systems and parallel processing

Simulate Hadoop-style distributed processing:
- [`simulated_hdfs.py`](03-distributed-simulation/simulated_hdfs.py) - HDFS simulation (blocks, replication)
- [`parallel_mapreduce.py`](03-distributed-simulation/parallel_mapreduce.py) - Parallel Map and Reduce tasks
- [`distributed_mapreduce.py`](03-distributed-simulation/distributed_mapreduce.py) - Complete distributed system

**Key Concepts**: Data blocks, replication, parallel processing, data locality

```bash
cd 03-distributed-simulation
python simulated_hdfs.py          # HDFS demo
python parallel_mapreduce.py      # Parallel processing demo
python distributed_mapreduce.py   # Full system demo
```

---

## Quick Start Guide

**Complete beginner?** Follow this path:

1. **Start with basics** (15-20 min)
   ```bash
   cd 01-basics
   python wordcount.py
   python examples/sales.py
   ```

2. **Move to file processing** (10-15 min)
   ```bash
   cd ../02-file-processing
   python wordcount_file.py ../../../../datasets/mapreduce/sample_text.txt
   python wordcount_multifile.py ../../../../datasets/mapreduce/
   ```

3. **Explore distributed concepts** (20-30 min)
   ```bash
   cd ../03-distributed-simulation
   python distributed_mapreduce.py
   ```

## What You'll Learn

- ✅ **Level 1**: How MapReduce works (map, shuffle, reduce)
- ✅ **Level 2**: Processing real data from files
- ✅ **Level 3**: How Hadoop distributes and parallelizes work

## Requirements

- Python 3.8+
- No external libraries needed (uses only standard library)

## Next Steps

After completing this module:
1. **`../02-mrjob/`** - Write Hadoop-compatible MapReduce code
2. **`../03-pyspark/`** - Use production-ready Big Data tools

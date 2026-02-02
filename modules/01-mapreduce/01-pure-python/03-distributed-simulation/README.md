# Level 3: Distributed Simulation

## Overview

Simulate how Hadoop distributes and processes data across a cluster. This level demonstrates the key concepts that make MapReduce scalable to Big Data.

## Files

### 1. HDFS Simulation
**[`simulated_hdfs.py`](simulated_hdfs.py)** - Distributed file system
- Splits files into blocks (like HDFS)
- Replicates blocks across multiple "nodes"
- Stores metadata about file locations
- Simulates data distribution
- Run: `python simulated_hdfs.py`

**Key Concepts:**
- Block storage (files split into chunks)
- Replication (3 copies of each block)
- Data locality (processing near data)
- Fault tolerance (multiple copies)

### 2. Parallel Processing
**[`parallel_mapreduce.py`](parallel_mapreduce.py)** - Multi-process execution
- Runs multiple Map tasks in parallel
- Implements Shuffle/Sort phase
- Runs multiple Reduce tasks in parallel
- Shows timing for each phase
- Run: `python parallel_mapreduce.py`

**Key Concepts:**
- Parallel Map tasks (using multiprocessing)
- Data partitioning across workers
- Parallel Reduce tasks
- Performance metrics

### 3. Complete System
**[`distributed_mapreduce.py`](distributed_mapreduce.py)** - Full Hadoop-style workflow
- Uploads files to HDFS (creates blocks)
- Reads blocks from distributed nodes
- Processes data in parallel
- Produces final results
- Run: `python distributed_mapreduce.py`

**Complete Workflow:**
1. Upload file → HDFS splits into blocks
2. Blocks replicated across nodes
3. Map tasks process blocks in parallel
4. Shuffle redistributes data by key
5. Reduce tasks aggregate in parallel
6. Final results produced

## Try It

```bash
# 1. See how HDFS works
python simulated_hdfs.py

# 2. See parallel processing
python parallel_mapreduce.py

# 3. Run the complete system
python distributed_mapreduce.py
```

## What You'll See

**HDFS Simulation:**
```
[HDFS] Uploading file.txt to /data/file.txt
  Block 0: 512 bytes -> ['node_0/...', 'node_1/...', 'node_2/...']
  Block 1: 512 bytes -> ['node_0/...', 'node_1/...', 'node_2/...']
```

**Parallel Processing:**
```
[PHASE 1] MAP - Parallel Execution
  [Map Task 0] Processing 200 records...
  [Map Task 1] Processing 200 records...
  [Map Task 2] Processing 200 records...
  
[PHASE 2] SHUFFLE & SORT
  Grouped into 150 unique keys
  
[PHASE 3] REDUCE - Parallel Execution
  [Reduce Task 0] Processing 75 keys...
  [Reduce Task 1] Processing 75 keys...
```

## Key Concepts

**Data Distribution**
- Files split into blocks
- Blocks stored across multiple nodes
- Enables parallel processing

**Replication**
- Each block stored 3 times (default)
- Provides fault tolerance
- Allows data locality optimization

**Parallel Execution**
- Multiple Map tasks run simultaneously
- Multiple Reduce tasks run simultaneously
- Dramatically faster than sequential processing

**Scalability**
- Add more nodes → process more data
- Add more workers → process faster
- This is how Hadoop handles petabytes

## How This Relates to Real Hadoop

| Simulation | Real Hadoop |
|------------|-------------|
| `simulated_hdfs.py` | HDFS (Hadoop Distributed File System) |
| `parallel_mapreduce.py` | YARN (resource manager) + MapReduce |
| Local directories as "nodes" | Actual cluster machines |
| Python multiprocessing | Distributed processes across cluster |
| 3 replicas | Configurable replication factor |

## Next Steps

You now understand:
- ✅ How MapReduce works (Level 1)
- ✅ How to process files (Level 2)
- ✅ How distributed systems work (Level 3)

**Ready for production tools?**
1. **`../../02-mrjob/`** - Write code that runs on real Hadoop
2. **`../../03-pyspark/`** - Use Apache Spark (faster than MapReduce)

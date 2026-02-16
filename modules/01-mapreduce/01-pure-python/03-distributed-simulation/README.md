# Level 3: Distributed Simulation

## Overview

Simulate how Hadoop distributes and processes data across a cluster. This level demonstrates the key concepts that make MapReduce scalable to Big Data.

## Files

### 1. HDFS Simulation
**[`simulated_hdfs.py`](simulated_hdfs.py)** - Distributed file system
- Splits files into blocks (like HDFS)
- Replicates blocks across multiple "nodes"
- Stores metadata about file locations
- Simulates data distribution across a cluster
- Run: `python3 simulated_hdfs.py`

**Key Concepts:**
- Block storage (files split into 256KB chunks)
- Replication (3 copies of each block by default)
- Data distribution (blocks spread across 4 nodes)
- Fault tolerance (multiple copies ensure availability)

**Demo Output:**
```
Demo mode: Using dataset file...
Usage: python simulated_hdfs.py <file1> [file2] ...

[HDFS] Uploading .../datasets/book/The story of the universe.txt to /user/data/the_story_of_the_universe.txt
  Block 0: 262144 bytes -> ['node_2/...', 'node_0/...', 'node_3/...']
  Block 1: 262144 bytes -> ['node_1/...', 'node_3/...', 'node_0/...']
  Block 2: 223637 bytes -> ['node_0/...', 'node_2/...', 'node_1/...']
[HDFS] Upload complete: 3 blocks, 747925 bytes
```
Notice how each block is on different nodes!

---

### 2. Parallel Processing
**[`parallel_mapreduce.py`](parallel_mapreduce.py)** - Multi-process execution
- Reads file directly (no HDFS)
- Runs multiple Map tasks in parallel
- Implements Shuffle/Sort phase
- Runs multiple Reduce tasks in parallel
- Shows timing for each phase
- Usage: `python3 parallel_mapreduce.py <filename>`

**Key Concepts:**
- Parallel Map tasks (using multiprocessing)
- Data partitioning across workers
- Parallel Reduce tasks
- Performance metrics

**Example:**
```bash
python3 parallel_mapreduce.py ../../../../datasets/book/The\ story\ of\ the\ universe.txt
```

**Demo Output:**
```
[PHASE 1] MAP - Parallel Execution
  [Map Task 0] Produced 30770 key-value pairs
  [Map Task 1] Produced 31803 key-value pairs
  [Map Task 2] Produced 32509 key-value pairs
  [Map Task 3] Produced 27063 key-value pairs
  
Map phase:     0.478s (48.1%)
Shuffle phase: 0.017s (1.7%)
Reduce phase:  0.499s (50.2%)
Total time:    0.994s
```

---

### 3. Complete System
**[`distributed_mapreduce.py`](distributed_mapreduce.py)** - Full Hadoop-style workflow
- Uploads files to HDFS (creates blocks)
- Blocks replicated across nodes
- Reads blocks from distributed nodes
- Processes data in parallel
- Produces final results
- Usage: `python3 distributed_mapreduce.py <file1> [file2] ...`

**Complete Workflow:**
1. Upload file → HDFS splits into blocks (24KB each)
2. Blocks replicated across 6 nodes (3 replicas each)
3. Map tasks process blocks in parallel (4 workers)
4. Shuffle redistributes data by key
5. Reduce tasks aggregate in parallel (2 workers)
6. Final results produced

**Example:**
```bash
python3 distributed_mapreduce.py ../../../../datasets/book/The\ story\ of\ the\ universe.txt
```

**Demo Output:**
```
[HDFS] Uploading file to /data/file.txt
  Block 0: 24576 bytes -> ['node_3/...', 'node_0/...', 'node_5/...']
  Block 1: 24576 bytes -> ['node_0/...', 'node_2/...', 'node_4/...']
  ...
  Block 30: 10645 bytes -> ['node_0/...', 'node_2/...', 'node_5/...']
[HDFS] Upload complete: 31 blocks, 747925 bytes

[HDFS] Reading blocks from distributed nodes...
  Progress: 31/31 blocks read (100.0%)

[PHASE 1] MAP - Parallel Execution
  Total: 11992 records -> 122145 key-value pairs
  Time: 0.546s

TOP 10 WORDS:
the                  10873
of                    6288
and                   4078
```

---

## Try It

### Step 1: HDFS Simulation (Optional filename parameter)
See how files are split into blocks and distributed:

**Demo mode (no parameters):**
```bash
python3 simulated_hdfs.py
```
Loads the dataset file automatically and shows HDFS distribution.

**With your own file:**
```bash
# Syntax: python3 simulated_hdfs.py <file1> [file2] ...

# Example with one file:
python3 simulated_hdfs.py ../../../../datasets/book/The\ story\ of\ the\ universe.txt

# Example with multiple files:
python3 simulated_hdfs.py ../../../../datasets/mapreduce/sample_text.txt ../../../../datasets/mapreduce/sample_bigdata.txt
```

Then explore the created structure:
```bash
ls -la hdfs_storage/
ls -la hdfs_storage/node_0/
ls -la hdfs_storage/node_1/
```

---

### Step 2: Parallel Processing (Requires filename parameter)
See real multiprocessing in action with a file:
```bash
# Syntax: python3 parallel_mapreduce.py <filename>

# Example with the book:
python3 parallel_mapreduce.py ../../../../datasets/book/The\ story\ of\ the\ universe.txt

# Example with sample data:
python3 parallel_mapreduce.py ../../../../datasets/mapreduce/sample_text.txt
```

**What it does:**
- Reads the file directly (no HDFS)
- Splits work across 4 Map workers
- Processes in parallel
- Shows timing for each phase

---

### Step 3: Complete System (Requires one or more filenames)
Experience the full Hadoop workflow with HDFS + MapReduce:
```bash
# Syntax: python3 distributed_mapreduce.py <file1> [file2] [file3] ...

# Example with one file:
python3 distributed_mapreduce.py ../../../../datasets/book/The\ story\ of\ the\ universe.txt

# Example with multiple files:
python3 distributed_mapreduce.py ../../../../datasets/mapreduce/sample_text.txt ../../../../datasets/mapreduce/sample_bigdata.txt
```

**What it does:**
1. Uploads file(s) to HDFS (creates blocks)
2. Distributes blocks across 6 nodes
3. Reads blocks from distributed storage
4. Processes with parallel MapReduce
5. Shows combined results if multiple files

---

## Quick Reference

| Script | Parameters | Example |
|--------|-----------|---------|
| `simulated_hdfs.py` | `[file1] [file2] ...` (optional) | `python3 simulated_hdfs.py` or `python3 simulated_hdfs.py file.txt` |
| `parallel_mapreduce.py` | `<filename>` (required) | `python3 parallel_mapreduce.py file.txt` |
| `distributed_mapreduce.py` | `<file1> [file2] ...` (required) | `python3 distributed_mapreduce.py file1.txt file2.txt` |

---

## What You'll See

### HDFS Block Distribution
```
Block 0: node_2, node_0, node_3  ← Different nodes
Block 1: node_1, node_3, node_0  ← Different nodes
Block 2: node_0, node_2, node_1  ← Different nodes
```
Each block is on 3 different nodes (not all on the same nodes!)

### Parallel Execution
```
[Map Task 0] Processing 2998 records...  ← Running simultaneously
[Map Task 1] Processing 2998 records...  ← Running simultaneously
[Map Task 2] Processing 2998 records...  ← Running simultaneously
[Map Task 3] Processing 2998 records...  ← Running simultaneously
```

### Performance Breakdown
```
Map phase:     62.5% of time
Shuffle phase:  1.8% of time
Reduce phase:  35.8% of time
```

---

## Key Concepts

### Data Distribution
- Files split into blocks (256KB default in `simulated_hdfs.py`, 24KB in `distributed_mapreduce.py`)
- Blocks stored across multiple nodes (4 nodes in standalone HDFS, 6 in distributed MapReduce)
- Enables parallel processing

### Replication
- Each block stored 3 times (default)
- Provides fault tolerance
- Allows data locality optimization

### Parallel Execution
- Multiple Map tasks run simultaneously
- Multiple Reduce tasks run simultaneously
- Dramatically faster than sequential processing

### Data Locality
- Process data where it's stored (avoid network transfer)
- HDFS replicates blocks across nodes
- Map tasks can run on any node that has the data

### Scalability
- Add more nodes → store more data
- Add more workers → process faster
- This is how Hadoop handles petabytes

---

## Comparison: With vs Without HDFS

### `parallel_mapreduce.py` (Without HDFS)
- ✅ Simpler to understand
- ✅ Focuses on parallel processing
- ✅ Faster startup (no HDFS overhead)
- ❌ Doesn't show data distribution
- **Use when**: Learning about parallelism

### `distributed_mapreduce.py` (With HDFS)
- ✅ Complete Hadoop simulation
- ✅ Shows data distribution across nodes
- ✅ Demonstrates block replication
- ✅ More realistic workflow
- **Use when**: Understanding full system

---

## How This Relates to Real Hadoop

| Simulation | Real Hadoop |
|------------|-------------|
| `simulated_hdfs.py` | HDFS (Hadoop Distributed File System) |
| `parallel_mapreduce.py` | YARN + MapReduce (processing only) |
| `distributed_mapreduce.py` | Complete Hadoop stack |
| Local directories as "nodes" | Actual cluster machines |
| Python multiprocessing | Distributed processes across cluster |
| 4-6 nodes, 3 replicas | Configurable (100s of nodes, 3+ replicas) |
| 24KB-256KB blocks | 128MB blocks (default in Hadoop) |

---

## Exercises

1. **Experiment with block size**: Change `block_size` in `distributed_mapreduce.py` to 8192 (8KB) and see how many more blocks are created

2. **Vary parallelism**: Change `num_mappers=4` to `num_mappers=8` and compare execution times

3. **Test fault tolerance**: After running `distributed_mapreduce.py`, delete one node directory (`rm -rf hdfs_storage/node_0`) and observe that data is still available in other nodes

4. **Process your own files**: Try with different text files and compare word frequencies

5. **Multiple files**: Process several files together and see combined results

---

## Cleanup

After running the simulations:
```bash
rm -rf hdfs_storage sample_data
```

---

## Next Steps

You now understand:
- ✅ How MapReduce works (Level 1)
- ✅ How to process files (Level 2)
- ✅ How distributed systems work (Level 3)

**Ready for production tools?**
1. **`../../02-mrjob/`** - Write code that runs on real Hadoop clusters
2. **`../../03-pyspark/`** - Use Apache Spark (faster than MapReduce)

"""
Complete Distributed MapReduce Simulation

Combines HDFS simulation with parallel MapReduce processing.
Demonstrates the full Hadoop-style workflow:
1. Upload files to HDFS (split into blocks)
2. Process blocks in parallel with Map tasks
3. Shuffle and sort intermediate results
4. Reduce in parallel to produce final results
"""

import sys; import os; sys.path.append(os.path.dirname(__file__)); from simulated_hdfs import SimulatedHDFS
import sys; import os; sys.path.append(os.path.dirname(__file__)); from parallel_mapreduce import parallel_mapreduce
import os
from pathlib import Path


def distributed_mapreduce_from_hdfs(
    hdfs: SimulatedHDFS,
    hdfs_path: str,
    mapper,
    reducer,
    num_mappers: int = 4,
    num_reducers: int = 2
):
    """
    Execute MapReduce on data stored in simulated HDFS.
    
    Args:
        hdfs: SimulatedHDFS instance
        hdfs_path: Path to file in HDFS
        mapper: Map function
        reducer: Reduce function
        num_mappers: Number of parallel map tasks
        num_reducers: Number of parallel reduce tasks
    
    Returns:
        Final MapReduce results
    """
    print(f"\n{'='*70}")
    print(f"DISTRIBUTED MAPREDUCE WITH SIMULATED HDFS")
    print(f"{'='*70}\n")
    
    # Get file blocks from HDFS
    print(f"[HDFS] Reading file: {hdfs_path}")
    blocks = hdfs.get_blocks(hdfs_path)
    print(f"[HDFS] File has {len(blocks)} blocks\n")
    
    # Read blocks and convert to lines
    print("[HDFS] Reading blocks from distributed nodes...")
    all_data = []
    for block_info in blocks:
        # Simulate data locality: read from first replica
        replica = block_info['replicas'][0]
        block_data = hdfs.read_block(replica)
        
        # Decode and split into lines
        text = block_data.decode('utf-8')
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        all_data.extend(lines)
        
        print(f"  Block {block_info['block_id']} from {replica}: {len(lines)} lines")
    
    print(f"\n[HDFS] Total lines read: {len(all_data)}\n")
    
    # Execute parallel MapReduce
    results = parallel_mapreduce(
        all_data,
        mapper,
        reducer,
        num_mappers=num_mappers,
        num_reducers=num_reducers
    )
    
    return results


if __name__ == "__main__":
    # Initialize HDFS
    hdfs = SimulatedHDFS(
        base_dir="hdfs_storage",
        block_size=512,  # Small blocks to see distribution
        replication=3
    )
    
    # Create sample data files
    data_dir = Path("sample_data")
    data_dir.mkdir(exist_ok=True)
    
    # File 1: MapReduce concepts
    file1 = data_dir / "mapreduce.txt"
    with open(file1, 'w') as f:
        f.write("""MapReduce is a programming model for processing large data sets.
The model consists of two main functions: map and reduce.
Map functions process input data and emit intermediate key-value pairs.
Reduce functions aggregate values associated with the same key.
MapReduce enables parallel processing across distributed systems.
The framework handles data distribution, fault tolerance, and load balancing.
MapReduce was popularized by Google for web indexing and search.
""" * 10)  # Repeat to create larger file
    
    # File 2: Big Data concepts
    file2 = data_dir / "bigdata.txt"
    with open(file2, 'w') as f:
        f.write("""Big Data refers to extremely large datasets that require distributed processing.
Volume, velocity, and variety are the three main characteristics of Big Data.
Hadoop is a popular framework for Big Data processing using MapReduce.
HDFS provides distributed storage for Big Data applications.
Data locality optimization reduces network traffic in distributed systems.
Parallel processing enables efficient analysis of massive datasets.
""" * 10)
    
    # Upload files to HDFS
    print("\n" + "="*70)
    print("UPLOADING FILES TO HDFS")
    print("="*70)
    
    hdfs.put(str(file1), "/data/mapreduce.txt")
    hdfs.put(str(file2), "/data/bigdata.txt")
    
    # Define mapper and reducer for word count
    def mapper(line):
        """Extract words from line."""
        words = line.lower().split()
        return [(word.strip('.,!?;:"()[]{}'), 1) for word in words if word.strip('.,!?;:"()[]{}')]
    
    def reducer(key, values):
        """Sum word counts."""
        return sum(values)
    
    # Process first file
    print("\n" + "="*70)
    print("PROCESSING FILE 1: /data/mapreduce.txt")
    print("="*70)
    
    results1 = distributed_mapreduce_from_hdfs(
        hdfs,
        "/data/mapreduce.txt",
        mapper,
        reducer,
        num_mappers=3,
        num_reducers=2
    )
    
    print("\nTOP 10 WORDS IN FILE 1:")
    print("-" * 40)
    sorted_results = sorted(results1.items(), key=lambda x: x[1], reverse=True)[:10]
    for word, count in sorted_results:
        print(f"{word:20} {count:5}")
    
    # Process second file
    print("\n" + "="*70)
    print("PROCESSING FILE 2: /data/bigdata.txt")
    print("="*70)
    
    results2 = distributed_mapreduce_from_hdfs(
        hdfs,
        "/data/bigdata.txt",
        mapper,
        reducer,
        num_mappers=3,
        num_reducers=2
    )
    
    print("\nTOP 10 WORDS IN FILE 2:")
    print("-" * 40)
    sorted_results = sorted(results2.items(), key=lambda x: x[1], reverse=True)[:10]
    for word, count in sorted_results:
        print(f"{word:20} {count:5}")
    
    # Cleanup
    print("\n" + "="*70)
    print("DEMO COMPLETE")
    print("="*70)
    print(f"\nHDFS storage location: {hdfs.base_dir}")
    print(f"Sample data location: {data_dir}")
    print("\nTo clean up, delete these directories.")

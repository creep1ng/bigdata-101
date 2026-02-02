"""
Parallel MapReduce with Simulated HDFS

Simulates distributed MapReduce processing with:
- Data stored in simulated HDFS (blocks across nodes)
- Parallel map tasks (using multiprocessing)
- Shuffle and sort phase
- Parallel reduce tasks
"""

import multiprocessing as mp
from collections import defaultdict
from typing import Callable, List, Tuple, Any
import time


def parallel_mapreduce(
    data: List[Any],
    mapper: Callable[[Any], List[Tuple[Any, Any]]],
    reducer: Callable[[Any, List[Any]], Any],
    num_mappers: int = 4,
    num_reducers: int = 2
) -> dict:
    """
    Execute MapReduce with parallel processing.
    
    Args:
        data: List of items to process
        mapper: Function that transforms items into (key, value) pairs
        reducer: Function that aggregates values by key
        num_mappers: Number of parallel map tasks
        num_reducers: Number of parallel reduce tasks
    
    Returns:
        Dictionary with final results
    """
    print(f"\n{'='*60}")
    print(f"PARALLEL MAPREDUCE EXECUTION")
    print(f"{'='*60}")
    print(f"Input records: {len(data)}")
    print(f"Map tasks: {num_mappers}")
    print(f"Reduce tasks: {num_reducers}")
    print(f"{'='*60}\n")
    
    # PHASE 1: PARALLEL MAP
    print("[PHASE 1] MAP - Parallel Execution")
    start_time = time.time()
    
    # Split data into chunks for parallel processing
    chunk_size = len(data) // num_mappers
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    
    # Execute map tasks in parallel
    with mp.Pool(processes=num_mappers) as pool:
        map_results = pool.starmap(
            _map_task,
            [(i, chunk, mapper) for i, chunk in enumerate(chunks)]
        )
    
    # Flatten results
    mapped = []
    for result in map_results:
        mapped.extend(result)
    
    map_time = time.time() - start_time
    print(f"  Mapped {len(data)} records -> {len(mapped)} key-value pairs")
    print(f"  Time: {map_time:.3f}s\n")
    
    # PHASE 2: SHUFFLE AND SORT
    print("[PHASE 2] SHUFFLE & SORT")
    start_time = time.time()
    
    # Group by key
    shuffled = defaultdict(list)
    for key, value in mapped:
        shuffled[key].append(value)
    
    # Partition keys across reducers
    keys = list(shuffled.keys())
    reducer_partitions = [[] for _ in range(num_reducers)]
    for i, key in enumerate(keys):
        partition = i % num_reducers
        reducer_partitions[partition].append((key, shuffled[key]))
    
    shuffle_time = time.time() - start_time
    print(f"  Grouped into {len(shuffled)} unique keys")
    print(f"  Partitioned across {num_reducers} reducers")
    print(f"  Time: {shuffle_time:.3f}s\n")
    
    # PHASE 3: PARALLEL REDUCE
    print("[PHASE 3] REDUCE - Parallel Execution")
    start_time = time.time()
    
    # Execute reduce tasks in parallel
    with mp.Pool(processes=num_reducers) as pool:
        reduce_results = pool.starmap(
            _reduce_task,
            [(i, partition, reducer) for i, partition in enumerate(reducer_partitions)]
        )
    
    # Merge results
    final_results = {}
    for result in reduce_results:
        final_results.update(result)
    
    reduce_time = time.time() - start_time
    print(f"  Reduced to {len(final_results)} final results")
    print(f"  Time: {reduce_time:.3f}s\n")
    
    # SUMMARY
    total_time = map_time + shuffle_time + reduce_time
    print(f"{'='*60}")
    print(f"EXECUTION SUMMARY")
    print(f"{'='*60}")
    print(f"Map phase:     {map_time:.3f}s ({map_time/total_time*100:.1f}%)")
    print(f"Shuffle phase: {shuffle_time:.3f}s ({shuffle_time/total_time*100:.1f}%)")
    print(f"Reduce phase:  {reduce_time:.3f}s ({reduce_time/total_time*100:.1f}%)")
    print(f"Total time:    {total_time:.3f}s")
    print(f"{'='*60}\n")
    
    return final_results


def _map_task(task_id: int, data: List[Any], mapper: Callable) -> List[Tuple[Any, Any]]:
    """Execute a single map task."""
    print(f"  [Map Task {task_id}] Processing {len(data)} records...")
    results = []
    for item in data:
        results.extend(mapper(item))
    print(f"  [Map Task {task_id}] Produced {len(results)} key-value pairs")
    return results


def _reduce_task(task_id: int, partition: List[Tuple[Any, List[Any]]], reducer: Callable) -> dict:
    """Execute a single reduce task."""
    print(f"  [Reduce Task {task_id}] Processing {len(partition)} keys...")
    results = {}
    for key, values in partition:
        results[key] = reducer(key, values)
    print(f"  [Reduce Task {task_id}] Produced {len(results)} results")
    return results


if __name__ == "__main__":
    # Example: Word count with parallel processing
    
    def mapper(line):
        """Map function for word count."""
        words = line.lower().split()
        return [(word.strip('.,!?;:"()[]{}'), 1) for word in words if word.strip('.,!?;:"()[]{}')]
    
    def reducer(key, values):
        """Reduce function for word count."""
        return sum(values)
    
    # Generate sample data
    text = [
        "MapReduce is a programming model for processing large data sets",
        "The MapReduce framework consists of map and reduce functions",
        "Map functions process input data and produce intermediate results",
        "Reduce functions aggregate intermediate results to produce final output",
        "Parallel processing enables MapReduce to handle big data efficiently",
        "Data locality is important in MapReduce for performance optimization",
        "The shuffle phase redistributes data between map and reduce tasks",
        "MapReduce can process terabytes of data across thousands of machines",
    ] * 100  # Repeat to simulate larger dataset
    
    print(f"Dataset size: {len(text)} lines")
    
    # Execute parallel MapReduce
    results = parallel_mapreduce(
        text,
        mapper,
        reducer,
        num_mappers=4,
        num_reducers=2
    )
    
    # Show top results
    print("TOP 10 WORDS:")
    print("-" * 40)
    sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)[:10]
    for word, count in sorted_results:
        print(f"{word:20} {count:5}")

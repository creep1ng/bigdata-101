"""
Parallel MapReduce with Real Multiprocessing

Works on Windows, macOS, and Linux by using proper multiprocessing patterns.
"""

import multiprocessing as mp
from collections import defaultdict
from typing import List, Tuple, Any
import time
import os


# Global mapper and reducer (needed for multiprocessing serialization)
_global_mapper = None
_global_reducer = None


def _init_worker_map(mapper):
    """Initialize worker with mapper function."""
    global _global_mapper
    _global_mapper = mapper


def _init_worker_reduce(reducer):
    """Initialize worker with reducer function."""
    global _global_reducer
    _global_reducer = reducer


def _map_task(args):
    """Execute a single map task."""
    task_id, chunk = args
    results = []
    for item in chunk:
        results.extend(_global_mapper(item))
    return (task_id, results)


def _reduce_task(args):
    """Execute a single reduce task."""
    task_id, partition = args
    results = {}
    for key, values in partition:
        results[key] = _global_reducer(key, values)
    return (task_id, results)


def parallel_mapreduce(
    data: List[Any],
    mapper,
    reducer,
    num_mappers: int = 4,
    num_reducers: int = 2
) -> dict:
    """
    Execute MapReduce with real parallel processing.
    
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
    
    # Split data into chunks
    chunk_size = max(1, len(data) // num_mappers)
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    
    # Execute map tasks in parallel
    with mp.Pool(processes=num_mappers, initializer=_init_worker_map, initargs=(mapper,)) as pool:
        map_results = pool.map(_map_task, [(i, chunk) for i, chunk in enumerate(chunks)])
    
    # Flatten results
    mapped = []
    for task_id, results in sorted(map_results):
        print(f"  [Map Task {task_id}] Produced {len(results)} key-value pairs")
        mapped.extend(results)
    
    map_time = time.time() - start_time
    print(f"  Total: {len(data)} records -> {len(mapped)} key-value pairs")
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
    with mp.Pool(processes=num_reducers, initializer=_init_worker_reduce, initargs=(reducer,)) as pool:
        reduce_results = pool.map(_reduce_task, [(i, partition) for i, partition in enumerate(reducer_partitions)])
    
    # Merge results
    final_results = {}
    for task_id, results in sorted(reduce_results):
        print(f"  [Reduce Task {task_id}] Produced {len(results)} results")
        final_results.update(results)
    
    reduce_time = time.time() - start_time
    print(f"  Total: {len(final_results)} final results")
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


# Define mapper and reducer at module level for serialization
def word_mapper(line):
    """Map function for word count."""
    words = line.lower().split()
    return [(word.strip('.,!?;:"()[]{}'), 1) for word in words if word.strip('.,!?;:"()[]{}')]


def word_reducer(key, values):
    """Reduce function for word count."""
    return sum(values)


if __name__ == "__main__":
    import sys
    
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python parallel_mapreduce.py <filename>")
        print("\nExample:")
        print("  python parallel_mapreduce.py ../../../../datasets/book/The\\ story\\ of\\ the\\ universe.txt")
        sys.exit(1)
    
    filename = sys.argv[1]
    
    # Check if file exists
    if not os.path.exists(filename):
        print(f"Error: File not found: {filename}")
        sys.exit(1)
    
    # Read file
    print(f"Reading file: {filename}")
    with open(filename, 'r', encoding='utf-8') as f:
        text = [line.strip() for line in f if line.strip()]
    
    print(f"Dataset size: {len(text)} lines")
    
    # Execute parallel MapReduce
    results = parallel_mapreduce(
        text,
        word_mapper,
        word_reducer,
        num_mappers=4,
        num_reducers=2
    )
    
    # Show top results
    print("TOP 10 WORDS:")
    print("-" * 40)
    sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)[:10]
    for word, count in sorted_results:
        print(f"{word:20} {count:5}")

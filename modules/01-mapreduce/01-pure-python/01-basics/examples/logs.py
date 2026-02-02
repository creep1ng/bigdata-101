"""
Log analysis using MapReduce.

Counts errors by type in log files.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from mapreduce_framework import mapreduce, print_results


def mapper(log_entry):
    """
    Extracts the log level from each entry.
    
    Args:
        log_entry: Log line
    
    Yields:
        Tuple (level, 1) if the line contains a valid level
    """
    levels = ['ERROR', 'WARNING', 'INFO', 'DEBUG']
    for level in levels:
        if level in log_entry:
            yield (level, 1)
            break


def reducer(level, counts):
    """
    Sums the total occurrences of each log level.
    
    Args:
        level: Log level (ERROR, WARNING, etc.)
        counts: List of counts
    
    Returns:
        Total occurrences of the level
    """
    return sum(counts)


if __name__ == "__main__":
    # Sample logs
    logs = [
        "2026-02-01 10:15:23 INFO Server started on port 8080",
        "2026-02-01 10:15:45 INFO User login: user123",
        "2026-02-01 10:16:12 WARNING High memory usage: 85%",
        "2026-02-01 10:16:30 ERROR Database connection failed",
        "2026-02-01 10:16:31 ERROR Retry attempt 1 failed",
        "2026-02-01 10:16:35 INFO Database connection restored",
        "2026-02-01 10:17:00 DEBUG Processing request ID: 12345",
        "2026-02-01 10:17:15 WARNING Slow query detected: 2.5s",
        "2026-02-01 10:17:45 ERROR Timeout on external API call",
        "2026-02-01 10:18:00 INFO Request completed successfully",
        "2026-02-01 10:18:30 DEBUG Cache hit rate: 92%",
        "2026-02-01 10:19:00 WARNING Disk space below 20%",
    ]
    
    print(f"Analyzing {len(logs)} log entries...\n")
    
    # Execute MapReduce
    results = mapreduce(logs, mapper, reducer)
    
    # Show results
    print_results(results, "Log Count by Level")

"""
Process multiple text files with MapReduce.

Counts word frequency across multiple files and shows which files contain each word.
"""

import os
from collections import defaultdict
from mapreduce_framework import mapreduce, print_results


def mapper(file_data):
    """
    Transforms lines from a file into (word, filename) pairs.
    
    Args:
        file_data: Tuple (filename, line)
    
    Yields:
        Tuples (word, filename) for each word in the line
    """
    filename, line = file_data
    words = line.lower().split()
    for word in words:
        word = word.strip('.,!?;:"()[]{}')
        if word:
            yield (word, filename)


def reducer(word, filenames):
    """
    Aggregates information about where each word appears.
    
    Args:
        word: The word
        filenames: List of filenames where the word appears
    
    Returns:
        Dictionary with count and unique files
    """
    return {
        'count': len(filenames),
        'files': len(set(filenames)),
        'file_list': list(set(filenames))
    }


def read_files(directory):
    """Reads all .txt files from a directory."""
    data = []
    for filename in os.listdir(directory):
        if filename.endswith('.txt'):
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        data.append((filename, line))
    return data


if __name__ == "__main__":
    import sys
    
    # Check if directory was provided
    if len(sys.argv) < 2:
        print("Usage: python wordcount_multifile.py <directory>")
        print("\nUsing current directory")
        directory = "."
    else:
        directory = sys.argv[1]
    
    try:
        # Read all files
        data = read_files(directory)
        
        if not data:
            print(f"No .txt files found in {directory}")
            sys.exit(1)
        
        print(f"Processing directory: {directory}")
        print(f"Total lines: {len(data)}\n")
        
        # Execute MapReduce
        results = mapreduce(data, mapper, reducer)
        
        # Show top words
        print("\n" + "="*60)
        print("Top 15 Most Frequent Words")
        print("="*60)
        
        sorted_results = sorted(results.items(), key=lambda x: x[1]['count'], reverse=True)[:15]
        
        for word, info in sorted_results:
            print(f"{word:20} | Count: {info['count']:4} | Files: {info['files']}")
        
        print(f"\nTotal unique words: {len(results)}")
        print(f"Total words processed: {sum(r['count'] for r in results.values())}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

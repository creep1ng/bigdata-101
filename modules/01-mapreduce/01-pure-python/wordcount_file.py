"""
WordCount from text file - MapReduce example.

Reads a text file and counts word frequency.
"""

from mapreduce_framework import mapreduce, print_results


def mapper(line):
    """
    Transforms a text line into (word, 1) pairs.
    
    Args:
        line: Text line
    
    Yields:
        Tuples (word, 1) for each word in the line
    """
    words = line.lower().split()
    for word in words:
        # Clean basic punctuation
        word = word.strip('.,!?;:"()[]{}')
        if word:
            yield (word, 1)


def reducer(key, values):
    """
    Sums all counts for a word.
    
    Args:
        key: The word
        values: List of counts (all are 1)
    
    Returns:
        Total occurrences of the word
    """
    return sum(values)


def read_file(filename):
    """Reads a text file and returns lines."""
    with open(filename, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]


if __name__ == "__main__":
    import sys
    
    # Check if filename was provided
    if len(sys.argv) < 2:
        print("Usage: python wordcount_file.py <filename>")
        print("\nUsing sample file: sample_text.txt")
        filename = "sample_text.txt"
    else:
        filename = sys.argv[1]
    
    try:
        # Read file
        lines = read_file(filename)
        print(f"Processing file: {filename}")
        print(f"Total lines: {len(lines)}\n")
        
        # Execute MapReduce
        results = mapreduce(lines, mapper, reducer)
        
        # Show results
        print_results(results, "Word Count", limit=20)
        
        print(f"Total unique words: {len(results)}")
        print(f"Total words: {sum(results.values())}")
        
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found")
        sys.exit(1)

"""
WordCount - Classic MapReduce example.

Counts the frequency of each word in a text.
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


if __name__ == "__main__":
    # Sample data
    text = [
        "MapReduce is a programming model",
        "MapReduce processes large volumes of data",
        "The MapReduce model has two main phases",
        "The Map phase transforms the data",
        "The Reduce phase aggregates the results"
    ]
    
    print("Input text:")
    for line in text:
        print(f"  {line}")
    
    # Execute MapReduce
    results = mapreduce(text, mapper, reducer)
    
    # Show results
    print_results(results, "Word Count", limit=10)
    
    print(f"Total unique words: {len(results)}")
    print(f"Total words: {sum(results.values())}")

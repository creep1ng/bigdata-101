# MapReduce with Pure Python

## Concept

Basic MapReduce implementation using only native Python functions to understand the paradigm without external dependencies.

## Basic Components

### 1. Map Function
Transforms each input element into one or more (key, value) pairs.

### 2. Shuffle/Sort Function
Groups all values that have the same key.

### 3. Reduce Function
Processes all values associated with each key and produces a result.

## Learning Path

Follow this order to understand MapReduce progressively:

### 1. Framework ([`mapreduce_framework.py`](mapreduce_framework.py))
Start here to understand the core MapReduce implementation.
- Contains the main `mapreduce()` function
- Implements Map, Shuffle/Sort, and Reduce phases
- Provides utility functions for displaying results
- This is the foundation for all other examples

### 2. Basic Example ([`wordcount.py`](wordcount.py))
Classic WordCount example with inline text data.
- Simplest possible example
- Data is defined directly in the code
- Perfect for understanding mapper and reducer functions
- Run: `python wordcount.py`

### 3. Single File Processing ([`wordcount_file.py`](wordcount_file.py))
WordCount reading from a text file.
- Introduces file I/O operations
- Processes one file at a time
- Shows how to handle command-line arguments
- Run: `python wordcount_file.py sample_text.txt`

### 4. Multiple File Processing ([`wordcount_multifile.py`](wordcount_multifile.py))
Advanced example processing multiple files.
- Reads all .txt files from a directory
- Tracks which files contain each word
- More realistic Big Data scenario
- Run: `python wordcount_multifile.py .`

### 5. Practical Examples (`examples/`)
Real-world use cases beyond text processing:

#### [`sales.py`](examples/sales.py) - Sales Analysis
- Processes structured data (dictionaries)
- Calculates total sales by product
- Shows MapReduce with business data
- Run: `python examples/sales.py`

#### [`logs.py`](examples/logs.py) - Log Analysis
- Analyzes system logs
- Counts events by severity level
- Common use case in production systems
- Run: `python examples/logs.py`

## Sample Data Files

- [`sample_text.txt`](sample_text.txt): Text about MapReduce concepts
- [`sample_bigdata.txt`](sample_bigdata.txt): Text about Big Data technologies

## Quick Start

```bash
# 1. Start with the basic example
python wordcount.py

# 2. Try processing a file
python wordcount_file.py sample_text.txt

# 3. Process multiple files
python wordcount_multifile.py .

# 4. Explore practical examples
python examples/sales.py
python examples/logs.py
```

## Advantages of this Approach

- No additional installations required
- Transparent and easy-to-debug code
- Perfect for understanding data flow
- Foundation for understanding more complex implementations

## Next Steps

After mastering pure Python MapReduce:
1. Move to `../02-mrjob/` for Hadoop-compatible code
2. Then explore `../03-pyspark/` for production-ready tools

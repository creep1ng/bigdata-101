# Level 2: File Processing

## Overview

Learn to process real files with MapReduce. This level introduces file I/O and handling multiple data sources.

## Files

### Single File Processing
**[`wordcount_file.py`](wordcount_file.py)** - Read and process a text file
- Reads data from disk instead of inline code
- Handles command-line arguments
- More realistic than inline data
- Usage: `python wordcount_file.py <filename>`

### Multiple File Processing
**[`wordcount_multifile.py`](wordcount_multifile.py)** - Process entire directories
- Reads all `.txt` files from a directory
- Tracks which files contain each word
- Simulates batch processing in Big Data
- Usage: `python wordcount_multifile.py <directory>`

### Sample Data
- **Sample datasets** are located in `../../../../datasets/mapreduce/`
  - `sample_text.txt` - Text about MapReduce concepts
  - `sample_bigdata.txt` - Text about Big Data technologies

## Key Concepts

**File I/O**
- Reading data from disk
- Processing line by line
- Memory-efficient streaming

**Batch Processing**
- Processing multiple files together
- Aggregating results across sources
- Typical Big Data workflow

**Command-Line Interface**
- Accepting file paths as arguments
- Making scripts reusable
- Production-ready patterns

## Try It

```bash
# Process a single file (use datasets)
python wordcount_file.py ../../../../datasets/mapreduce/sample_text.txt

# Process all .txt files in datasets directory
python wordcount_multifile.py ../../../../datasets/mapreduce/

# Try with your own files
echo "Hello MapReduce World" > myfile.txt
python wordcount_file.py myfile.txt
```

## What's Different from Level 1?

- ✅ Data comes from files (not hardcoded)
- ✅ Can process any text file
- ✅ Can handle multiple files at once
- ✅ More like real Big Data workflows

## Next Level

Ready for distributed systems? Move to **Level 3** to learn about HDFS and parallel processing.

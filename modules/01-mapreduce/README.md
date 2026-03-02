# MapReduce - Fundamentals

## Introduction

MapReduce is a programming model for processing large volumes of data in a distributed manner. The paradigm is based on two main operations:

- **Map**: Transforms input data into key-value pairs
- **Reduce**: Groups and processes values associated with the same key

## Learning Approaches

This module covers two approaches to learn MapReduce locally:

### 1. Pure Python (Fundamentals)
- Implementation from scratch using native Python functions
- Goal: Understand the concept without external dependencies
- Ideal for: Understanding theory and data flow
- Location: `01-pure-python/`

### 2. mrjob (Hadoop Simulation)
- Library that simulates Hadoop behavior locally
- Goal: Write Hadoop-compatible code
- Ideal for: Transition to production environments
- Location: `02-mrjob/`
- **Status**: ✅ Complete - Ready to use!

## Recommended Progression

1. Start with pure Python to understand concepts
2. Practice with mrjob to see realistic implementations
3. Continue with the `02-spark` module for professional tools

## Requirements

- Python 3.8+
- pip for package installation
- Code editor or Jupyter Notebook

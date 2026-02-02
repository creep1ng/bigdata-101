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

## Included Examples

- `wordcount.py`: Word counter (classic example)
- `mapreduce_framework.py`: Reusable framework
- `examples/`: Additional use cases

## Execution

```bash
python wordcount.py
```

## Advantages of this Approach

- No additional installations required
- Transparent and easy-to-debug code
- Perfect for understanding data flow
- Foundation for understanding more complex implementations

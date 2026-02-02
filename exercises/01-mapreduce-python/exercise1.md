# Exercise 1: Temperature Analysis

## Objective
Implement a MapReduce program that calculates the average temperature by city.

## Input Data

```python
temperatures = [
    {'city': 'Medellin', 'temperature': 22, 'date': '2026-01-01'},
    {'city': 'Bogota', 'temperature': 14, 'date': '2026-01-01'},
    {'city': 'Medellin', 'temperature': 24, 'date': '2026-01-02'},
    {'city': 'Cali', 'temperature': 28, 'date': '2026-01-01'},
    {'city': 'Bogota', 'temperature': 13, 'date': '2026-01-02'},
    {'city': 'Cali', 'temperature': 30, 'date': '2026-01-02'},
    {'city': 'Medellin', 'temperature': 23, 'date': '2026-01-03'},
    {'city': 'Bogota', 'temperature': 15, 'date': '2026-01-03'},
]
```

## Tasks

1. Implement the `mapper` function that extracts (city, temperature)
2. Implement the `reducer` function that calculates the average temperature
3. Execute the MapReduce process and display results

## Expected Result

```
Medellin: 23.0°C
Bogota: 14.0°C
Cali: 29.0°C
```

## Hints

- To calculate average: `sum(values) / len(values)`
- Use the framework from `mapreduce_framework.py`
- You can base your solution on the examples in `sales.py` or `logs.py`

## Solution

The solution is in `solution_temperatures.py`

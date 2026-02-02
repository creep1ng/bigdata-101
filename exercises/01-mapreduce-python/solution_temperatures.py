"""
Solution: Temperature Analysis with MapReduce
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../modules/01-mapreduce/01-pure-python'))

from mapreduce_framework import mapreduce, print_results


def mapper(record):
    """Extracts city and temperature."""
    yield (record['city'], record['temperature'])


def reducer(city, temperatures):
    """Calculates average temperature."""
    return sum(temperatures) / len(temperatures)


if __name__ == "__main__":
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
    
    results = mapreduce(temperatures, mapper, reducer)
    
    print("\nAverage Temperature by City:")
    print("="*40)
    for city, temp in sorted(results.items()):
        print(f"{city}: {temp:.1f}°C")

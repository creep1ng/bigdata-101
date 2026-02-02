"""
Sales analysis using MapReduce.

Calculates total sales by product.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from mapreduce_framework import mapreduce, print_results


def mapper(sale):
    """
    Extracts product and amount from each sale.
    
    Args:
        sale: Dictionary with sale information
    
    Yields:
        Tuple (product, amount)
    """
    yield (sale['product'], sale['amount'])


def reducer(product, amounts):
    """
    Sums all sale amounts for a product.
    
    Args:
        product: Product name
        amounts: List of sale amounts
    
    Returns:
        Total sales for the product
    """
    return sum(amounts)


if __name__ == "__main__":
    # Sales data
    sales = [
        {'product': 'Laptop', 'amount': 1200, 'date': '2026-01-15'},
        {'product': 'Mouse', 'amount': 25, 'date': '2026-01-15'},
        {'product': 'Keyboard', 'amount': 75, 'date': '2026-01-16'},
        {'product': 'Laptop', 'amount': 1100, 'date': '2026-01-16'},
        {'product': 'Monitor', 'amount': 300, 'date': '2026-01-17'},
        {'product': 'Mouse', 'amount': 25, 'date': '2026-01-17'},
        {'product': 'Laptop', 'amount': 1250, 'date': '2026-01-18'},
        {'product': 'Keyboard', 'amount': 80, 'date': '2026-01-18'},
        {'product': 'Monitor', 'amount': 350, 'date': '2026-01-19'},
        {'product': 'Mouse', 'amount': 30, 'date': '2026-01-19'},
    ]
    
    print(f"Analyzing {len(sales)} sales...")
    
    # Execute MapReduce
    results = mapreduce(sales, mapper, reducer)
    
    # Show results
    print_results(results, "Total Sales by Product")
    
    print(f"Total sales: ${sum(results.values())}")

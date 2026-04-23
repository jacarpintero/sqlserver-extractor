---
name: python-performance-optimization
description: Profile and optimize Python code using cProfile, memory profilers, and performance best practices. Use when debugging slow Python code, optimizing bottlenecks, or improving application performance.
---

# Python Performance Optimization

Comprehensive guide to profiling, analyzing, and optimizing Python code for better performance, including CPU profiling, memory optimization, and implementation best practices.

## When to Use This Skill

- Identifying performance bottlenecks in Python applications
- Reducing application latency and response times
- Optimizing CPU-intensive operations
- Reducing memory consumption and memory leaks
- Improving database query performance
- Optimizing I/O operations
- Speeding up data processing pipelines
- Implementing high-performance algorithms
- Profiling production applications

## Core Concepts

### 1. Profiling Types

- **CPU Profiling**: Identify time-consuming functions
- **Memory Profiling**: Track memory allocation and leaks
- **Line Profiling**: Profile at line-by-line granularity
- **Call Graph**: Visualize function call relationships

### 2. Performance Metrics

- **Execution Time**: How long operations take
- **Memory Usage**: Peak and average memory consumption
- **CPU Utilization**: Processor usage patterns
- **I/O Wait**: Time spent on I/O operations

### 3. Optimization Strategies

- **Algorithmic**: Better algorithms and data structures
- **Implementation**: More efficient code patterns
- **Parallelization**: Multi-threading/processing
- **Caching**: Avoid redundant computation
- **Native Extensions**: C/Rust for critical paths

## Quick Start

### Basic Timing

```python
import time

def measure_time():
    """Simple timing measurement."""
    start = time.time()

    # Your code here
    result = sum(range(1000000))

    elapsed = time.time() - start
    print(f"Execution time: {elapsed:.4f} seconds")
    return result

# Better: use timeit for accurate measurements
import timeit

execution_time = timeit.timeit(
    "sum(range(1000000))",
    number=100
)
print(f"Average time: {execution_time/100:.6f} seconds")
```

## Profiling Tools

### Pattern 1: cProfile - CPU Profiling

```python
import cProfile
import pstats
from pstats import SortKey

def slow_function():
    total = 0
    for i in range(1000000):
        total += i
    return total

def another_function():
    return [i**2 for i in range(100000)]

def main():
    result1 = slow_function()
    result2 = another_function()
    return result1, result2

if __name__ == "__main__":
    profiler = cProfile.Profile()
    profiler.enable()

    main()

    profiler.disable()

    stats = pstats.Stats(profiler)
    stats.sort_stats(SortKey.CUMULATIVE)
    stats.print_stats(10)  # Top 10 functions

    stats.dump_stats("profile_output.prof")
```

**Command-line profiling:**

```bash
python -m cProfile -o output.prof script.py

python -m pstats output.prof
# sort cumtime
# stats 10
```

### Pattern 2: line_profiler - Line-by-Line Profiling

```python
# pip install line-profiler

@profile
def process_data(data):
    result = []
    for item in data:
        processed = item * 2
        result.append(processed)
    return result

# Run with: kernprof -l -v script.py
```

**Manual line profiling:**

```python
from line_profiler import LineProfiler

def process_data(data):
    result = []
    for item in data:
        processed = item * 2
        result.append(processed)
    return result

if __name__ == "__main__":
    lp = LineProfiler()
    lp.add_function(process_data)
    data = list(range(100000))
    lp_wrapper = lp(process_data)
    lp_wrapper(data)
    lp.print_stats()
```

### Pattern 3: memory_profiler - Memory Usage

```python
# pip install memory-profiler

from memory_profiler import profile

@profile
def memory_intensive():
    big_list = [i for i in range(1000000)]
    big_dict = {i: i**2 for i in range(100000)}
    result = sum(big_list)
    return result

# Run with: python -m memory_profiler script.py
```

### Pattern 4: py-spy - Production Profiling

```bash
# pip install py-spy

py-spy top --pid 12345
py-spy record -o profile.svg --pid 12345
py-spy record -o profile.svg -- python script.py
py-spy dump --pid 12345
```

## Optimization Patterns

### Pattern 5: List Comprehensions vs Loops

```python
import timeit

def slow_squares(n):
    result = []
    for i in range(n):
        result.append(i**2)
    return result

def fast_squares(n):
    return [i**2 for i in range(n)]

n = 100000
slow_time = timeit.timeit(lambda: slow_squares(n), number=100)
fast_time = timeit.timeit(lambda: fast_squares(n), number=100)
print(f"Speedup: {slow_time/fast_time:.2f}x")
```

### Pattern 6: Generator Expressions for Memory

```python
import sys

list_data = [i for i in range(1000000)]
gen_data = (i for i in range(1000000))

print(f"List size: {sys.getsizeof(list_data)} bytes")
print(f"Generator size: {sys.getsizeof(gen_data)} bytes")
```

### Pattern 7: String Concatenation

```python
def slow_concat(items):
    result = ""
    for item in items:
        result += str(item)
    return result

def fast_concat(items):
    return "".join(str(item) for item in items)
```

### Pattern 8: Dictionary Lookups vs List Searches

```python
# O(n) list search
target in items

# O(1) dict/set lookup
target in lookup_dict
```

### Pattern 9: Local Variable Access

```python
def use_local():
    local_value = GLOBAL_VALUE  # cache global as local
    total = 0
    for i in range(10000):
        total += local_value
    return total
```

### Pattern 10: Caching with lru_cache

```python
from functools import lru_cache

@lru_cache(maxsize=None)
def fibonacci(n):
    if n < 2:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)
```

## Best Practices

1. **Profile before optimizing** - Measure to find real bottlenecks
2. **Focus on hot paths** - Optimize code that runs most frequently
3. **Use appropriate data structures** - Dict for lookups, set for membership
4. **Avoid premature optimization** - Clarity first, then optimize
5. **Use built-in functions** - They're implemented in C
6. **Cache expensive computations** - Use lru_cache
7. **Batch I/O operations** - Reduce system calls
8. **Use generators** for large datasets
9. **Consider NumPy** for numerical operations
10. **Profile production code** - Use py-spy for live systems

## Common Pitfalls

- Optimizing without profiling
- Using global variables unnecessarily
- Not using appropriate data structures
- Creating unnecessary copies of data
- Not using connection pooling for databases
- Ignoring algorithmic complexity
- Over-optimizing rare code paths
- Not considering memory usage

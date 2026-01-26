# Hop Traversal Benchmark Guide

This benchmark script measures the performance of 1-hop and 2-hop graph traversals in Apache AGE.

## What It Tests

The script benchmarks four types of queries:

1. **1-hop directed**: Find all nodes that can be reached from a starting node by following one edge in the forward direction
2. **2-hop directed**: Find all nodes that can be reached by following two edges in the forward direction
3. **1-hop undirected**: Find all nodes connected by one edge in either direction
4. **2-hop undirected**: Find all nodes within 2 hops in either direction

## Usage

### Basic Usage (Test 5 Random Nodes)

```bash
python benchmark_hops.py
```

This will:
- Select 5 random nodes from your graph
- Run 10 iterations of each query type per node
- Calculate statistics (min, max, mean, median, std dev)
- Show aggregate results across all test nodes

### Custom Number of Test Nodes

```bash
# Test 10 random nodes
python benchmark_hops.py --num-nodes 10

# Test just 1 random node
python benchmark_hops.py --num-nodes 1
```

### More Iterations for Better Statistics

```bash
# Run 20 iterations per query (more accurate)
python benchmark_hops.py --iterations 20

# Run 50 iterations
python benchmark_hops.py --iterations 50
```

### Test a Specific Node

```bash
# Test node with ID 42
python benchmark_hops.py --node-id 42

# Test node 100 with 20 iterations
python benchmark_hops.py --node-id 100 --iterations 20
```

### Custom Graph Name

```bash
# If your graph isn't named 'generated_graph'
python benchmark_hops.py --graph-name my_graph
```

### Combined Options

```bash
# Comprehensive benchmark: 10 nodes, 20 iterations each
python benchmark_hops.py --num-nodes 10 --iterations 20
```

## Example Output

```
======================================================================
HOP TRAVERSAL BENCHMARK
======================================================================

Graph: generated_graph
Iterations per query: 10
Number of test nodes: 5

Getting random node IDs for testing...
Testing with nodes: [42, 158, 73, 201, 15]

──────────────────────────────────────────────────────────────────────
Testing node 42 (1/5)
──────────────────────────────────────────────────────────────────────

  Running 1-hop directed queries...
    Results: 3 nodes found
    Time: 12.45ms (avg), 12.20ms (median)

  Running 2-hop directed queries...
    Results: 27 nodes found
    Time: 45.32ms (avg), 44.80ms (median)

  Running 1-hop undirected queries...
    Results: 5 nodes found
    Time: 15.67ms (avg), 15.40ms (median)

  Running 2-hop undirected queries...
    Results: 48 nodes found
    Time: 78.23ms (avg), 77.50ms (median)

[... continues for other nodes ...]

======================================================================
AGGREGATE RESULTS (across all test nodes)
======================================================================

1-HOP:
  Average time: 11.23ms
  Median time: 11.10ms
  Min time: 8.45ms
  Max time: 15.67ms
  Std dev: 2.34ms
  Avg results returned: 3.2 nodes
  Min results: 1 nodes
  Max results: 7 nodes

2-HOP:
  Average time: 42.56ms
  Median time: 41.23ms
  Min time: 28.34ms
  Max time: 67.89ms
  Std dev: 12.45ms
  Avg results returned: 25.4 nodes
  Min results: 8 nodes
  Max results: 52 nodes

======================================================================
PERFORMANCE SUMMARY
======================================================================

Directed Traversal:
  1-hop average: 11.23ms
  2-hop average: 42.56ms
  2-hop is 3.8x slower than 1-hop

Undirected Traversal:
  1-hop average: 14.56ms
  2-hop average: 73.21ms
  2-hop is 5.0x slower than 1-hop
```

## Understanding the Results

### Key Metrics

- **Min/Max**: Fastest and slowest query times
- **Mean**: Average query time
- **Median**: Middle value (less affected by outliers)
- **Std Dev**: How much variation in query times
- **Results returned**: Number of nodes found

### What to Look For

**Good Performance:**
- 1-hop queries: < 20ms
- 2-hop queries: < 100ms
- Low standard deviation (consistent performance)

**Performance Issues:**
- High standard deviation (inconsistent)
- 2-hop queries > 1000ms
- Large variation between min and max

### Performance Factors

Query time depends on:
1. **Graph density**: More edges = slower traversals
2. **Node degree**: High-degree nodes slower
3. **Graph size**: Larger graphs generally slower
4. **Indexes**: Make sure indexes exist on node IDs
5. **Hardware**: CPU, RAM, disk speed

## Improving Performance

### 1. Create Indexes (If Not Already)

Indexes on node IDs significantly improve performance:

```python
from load_to_age import create_indexes
create_indexes('generated_graph')
```

### 2. PostgreSQL Tuning

Edit PostgreSQL config for better query performance:

```bash
# In postgresql.conf
shared_buffers = 2GB
work_mem = 256MB
effective_cache_size = 6GB
random_page_cost = 1.1  # For SSD
```

### 3. Add More Specific Indexes

If querying specific edge types frequently:

```sql
CREATE INDEX idx_edge_type ON generated_graph."WORKS_AT" (start_id, end_id);
```

### 4. Limit Result Sets

For very large result sets, add LIMIT:

```cypher
MATCH (start {id: 42})-[]->()-[]->(connected)
RETURN DISTINCT connected
LIMIT 100
```

## Benchmark Different Scenarios

### Small Graph (1K nodes, 5K edges)

```bash
# Expected: 1-hop ~5-10ms, 2-hop ~15-30ms
python benchmark_hops.py --num-nodes 5 --iterations 10
```

### Medium Graph (10K nodes, 50K edges)

```bash
# Expected: 1-hop ~10-20ms, 2-hop ~40-80ms
python benchmark_hops.py --num-nodes 10 --iterations 20
```

### Large Graph (100K+ nodes, 500K+ edges)

```bash
# Expected: 1-hop ~15-30ms, 2-hop ~60-150ms
python benchmark_hops.py --num-nodes 10 --iterations 10
```

### High-Degree Nodes

Test nodes with many connections:

```bash
# Find high-degree node first, then test it
python benchmark_hops.py --node-id <high_degree_node_id> --iterations 20
```

## Exporting Results

Save results to a file:

```bash
python benchmark_hops.py --num-nodes 10 --iterations 20 > benchmark_results.txt
```

## Comparing Different Graphs

```bash
# Benchmark graph 1
python benchmark_hops.py --graph-name graph1 > results_graph1.txt

# Benchmark graph 2
python benchmark_hops.py --graph-name graph2 > results_graph2.txt

# Compare the files
```

## Troubleshooting

### "No nodes found"

Make sure your graph has data:
```bash
python quick_check.py
```

### Very slow queries

1. Check graph size:
   ```bash
   python quick_check.py
   ```

2. Check if indexes exist:
   ```python
   from load_to_age import create_indexes
   create_indexes()
   ```

3. Check for high-degree nodes:
   ```cypher
   MATCH (n)-[r]->()
   RETURN n.id, count(r) as degree
   ORDER BY degree DESC
   LIMIT 10
   ```

### Inconsistent results

- Increase iterations: `--iterations 50`
- Close other applications
- Restart PostgreSQL container:
  ```bash
  docker restart age-container
  ```

## Use Cases

**Development:**
- Test query performance during development
- Compare before/after optimization
- Identify performance bottlenecks

**Production:**
- Monitor query performance over time
- Establish performance baselines
- Identify degradation as graph grows

**Research:**
- Study how graph structure affects query performance
- Compare different graph densities
- Analyze scaling behavior

## Tips

1. **Run multiple times**: Performance can vary, run several benchmarks
2. **Warm up**: First query may be slower (caching), consider discarding
3. **Consistent environment**: Close other applications during benchmarking
4. **Document results**: Save results with graph size info for comparison
5. **Test edge cases**: Both sparse and dense graph regions

Happy benchmarking! 🚀

# benchmark_hops.py
"""
Benchmark script to measure 1-hop and 2-hop query performance in Apache AGE.
"""

import time
import statistics
from db_connection import get_connection
from config import GRAPH_NAME

def run_query_with_timing(cur, query, description):
    """Execute a query and return execution time."""
    start_time = time.time()
    cur.execute(query)
    results = cur.fetchall()
    elapsed_time = time.time() - start_time
    return elapsed_time, len(results)

def benchmark_1_hop(graph_name=GRAPH_NAME, node_id=1, iterations=10):
    """
    Benchmark 1-hop traversal: Find all nodes connected to a given node.
    
    Query: MATCH (start {id: node_id})-[]->(connected) RETURN connected
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        times = []
        result_counts = []
        
        for i in range(iterations):
            query = f"""
            SELECT * FROM cypher('{graph_name}', $$
                EXPLAIN
                MATCH (start {{id: {node_id}}})-[]->(connected)
                RETURN connected
            $$) as (node agtype);
            """
            
            elapsed, count = run_query_with_timing(cur, query, "1-hop")
            times.append(elapsed)
            result_counts.append(count)
        
        return {
            'min': min(times) * 1000,  # Convert to milliseconds
            'max': max(times) * 1000,
            'mean': statistics.mean(times) * 1000,
            'median': statistics.median(times) * 1000,
            'stdev': statistics.stdev(times) * 1000 if len(times) > 1 else 0,
            'result_count': result_counts[0]  # Should be same for all iterations
        }
    finally:
        cur.close()
        conn.close()

def benchmark_2_hop(graph_name=GRAPH_NAME, node_id=1, iterations=10):
    """
    Benchmark 2-hop traversal: Find all nodes within 2 hops of a given node.
    
    Query: MATCH (start {id: node_id})-[]->()-[]->(connected) RETURN DISTINCT connected
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        times = []
        result_counts = []
        
        for i in range(iterations):
            query = f"""
            SELECT * FROM cypher('{graph_name}', $$
                MATCH (start {{id: {node_id}}})-[]->()-[]->(connected)
                RETURN DISTINCT connected
            $$) as (node agtype);
            """
            
            elapsed, count = run_query_with_timing(cur, query, "2-hop")
            times.append(elapsed)
            result_counts.append(count)
        
        return {
            'min': min(times) * 1000,
            'max': max(times) * 1000,
            'mean': statistics.mean(times) * 1000,
            'median': statistics.median(times) * 1000,
            'stdev': statistics.stdev(times) * 1000 if len(times) > 1 else 0,
            'result_count': result_counts[0]
        }
    finally:
        cur.close()
        conn.close()

def benchmark_undirected_1_hop(graph_name=GRAPH_NAME, node_id=1, iterations=10):
    """
    Benchmark 1-hop undirected traversal: Find all nodes connected in either direction.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        times = []
        result_counts = []
        
        for i in range(iterations):
            query = f"""
            SELECT * FROM cypher('{graph_name}', $$
                EXPLAIN
                MATCH (start {{id: {node_id}}})-[]-(connected)
                RETURN DISTINCT connected
            $$) as (node agtype);
            """
            
            elapsed, count = run_query_with_timing(cur, query, "1-hop undirected")
            times.append(elapsed)
            result_counts.append(count)
        
        return {
            'min': min(times) * 1000,
            'max': max(times) * 1000,
            'mean': statistics.mean(times) * 1000,
            'median': statistics.median(times) * 1000,
            'stdev': statistics.stdev(times) * 1000 if len(times) > 1 else 0,
            'result_count': result_counts[0]
        }
    finally:
        cur.close()
        conn.close()

def benchmark_undirected_2_hop(graph_name=GRAPH_NAME, node_id=1, iterations=10):
    """
    Benchmark 2-hop undirected traversal: Find all nodes within 2 hops in either direction.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        times = []
        result_counts = []
        
        for i in range(iterations):
            query = f"""
            SELECT * FROM cypher('{graph_name}', $$
                MATCH (start {{id: {node_id}}})-[]-()-[]-(connected)
                RETURN DISTINCT connected
            $$) as (node agtype);
            """
            
            elapsed, count = run_query_with_timing(cur, query, "2-hop undirected")
            times.append(elapsed)
            result_counts.append(count)
        
        return {
            'min': min(times) * 1000,
            'max': max(times) * 1000,
            'mean': statistics.mean(times) * 1000,
            'median': statistics.median(times) * 1000,
            'stdev': statistics.stdev(times) * 1000 if len(times) > 1 else 0,
            'result_count': result_counts[0]
        }
    finally:
        cur.close()
        conn.close()

def get_random_node_ids(graph_name=GRAPH_NAME, count=5):
    """Get random node IDs from the graph."""
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        query = f"""
        SELECT * FROM cypher('{graph_name}', $$
            MATCH (n)
            RETURN n.id as id
            LIMIT {count * 10}
        $$) as (id agtype);
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        # Extract node IDs and return subset
        import random
        node_ids = [int(str(row[0]).strip('"')) for row in results]
        return random.sample(node_ids, min(count, len(node_ids)))
        
    finally:
        cur.close()
        conn.close()

def run_benchmark_suite(graph_name=GRAPH_NAME, iterations=10, num_nodes=5):
    """Run complete benchmark suite."""
    
    print("="*70)
    print("HOP TRAVERSAL BENCHMARK")
    print("="*70)
    print(f"\nGraph: {graph_name}")
    print(f"Iterations per query: {iterations}")
    print(f"Number of test nodes: {num_nodes}")
    
    # Get random node IDs to test
    print("\nGetting random node IDs for testing...")
    node_ids = get_random_node_ids(graph_name, num_nodes)
    print(f"Testing with nodes: {node_ids}")
    
    # Store results
    all_results = {
        '1-hop': [],
        '2-hop': [],
        '1-hop-undirected': [],
        '2-hop-undirected': []
    }
    
    # Run benchmarks for each node
    for i, node_id in enumerate(node_ids, 1):
        print(f"\n{'─'*70}")
        print(f"Testing node {node_id} ({i}/{num_nodes})")
        print(f"{'─'*70}")
        
        # 1-hop directed
        print("\n  Running 1-hop directed queries...")
        result = benchmark_1_hop(graph_name, node_id, iterations)
        all_results['1-hop'].append(result)
        print(f"    Results: {result['result_count']} nodes found")
        print(f"    Time: {result['mean']:.2f}ms (avg), {result['median']:.2f}ms (median)")
        
        # 2-hop directed
        print("\n  Running 2-hop directed queries...")
        result = benchmark_2_hop(graph_name, node_id, iterations)
        all_results['2-hop'].append(result)
        print(f"    Results: {result['result_count']} nodes found")
        print(f"    Time: {result['mean']:.2f}ms (avg), {result['median']:.2f}ms (median)")
        
        # 1-hop undirected
        print("\n  Running 1-hop undirected queries...")
        result = benchmark_undirected_1_hop(graph_name, node_id, iterations)
        all_results['1-hop-undirected'].append(result)
        print(f"    Results: {result['result_count']} nodes found")
        print(f"    Time: {result['mean']:.2f}ms (avg), {result['median']:.2f}ms (median)")
        
        # 2-hop undirected
        print("\n  Running 2-hop undirected queries...")
        result = benchmark_undirected_2_hop(graph_name, node_id, iterations)
        all_results['2-hop-undirected'].append(result)
        print(f"    Results: {result['result_count']} nodes found")
        print(f"    Time: {result['mean']:.2f}ms (avg), {result['median']:.2f}ms (median)")
    
    # Calculate aggregate statistics
    print("\n" + "="*70)
    print("AGGREGATE RESULTS (across all test nodes)")
    print("="*70)
    
    for query_type, results in all_results.items():
        if results:
            means = [r['mean'] for r in results]
            medians = [r['median'] for r in results]
            result_counts = [r['result_count'] for r in results]
            
            print(f"\n{query_type.upper()}:")
            print(f"  Average time: {statistics.mean(means):.2f}ms")
            print(f"  Median time: {statistics.median(medians):.2f}ms")
            print(f"  Min time: {min(means):.2f}ms")
            print(f"  Max time: {max(means):.2f}ms")
            print(f"  Std dev: {statistics.stdev(means):.2f}ms" if len(means) > 1 else "  Std dev: N/A")
            print(f"  Avg results returned: {statistics.mean(result_counts):.1f} nodes")
            print(f"  Min results: {min(result_counts)} nodes")
            print(f"  Max results: {max(result_counts)} nodes")
    
    # Performance summary
    print("\n" + "="*70)
    print("PERFORMANCE SUMMARY")
    print("="*70)
    
    if all_results['1-hop'] and all_results['2-hop']:
        hop1_avg = statistics.mean([r['mean'] for r in all_results['1-hop']])
        hop2_avg = statistics.mean([r['mean'] for r in all_results['2-hop']])
        
        print(f"\nDirected Traversal:")
        print(f"  1-hop average: {hop1_avg:.2f}ms")
        print(f"  2-hop average: {hop2_avg:.2f}ms")
        print(f"  2-hop is {hop2_avg/hop1_avg:.1f}x slower than 1-hop")
    
    if all_results['1-hop-undirected'] and all_results['2-hop-undirected']:
        hop1_avg = statistics.mean([r['mean'] for r in all_results['1-hop-undirected']])
        hop2_avg = statistics.mean([r['mean'] for r in all_results['2-hop-undirected']])
        
        print(f"\nUndirected Traversal:")
        print(f"  1-hop average: {hop1_avg:.2f}ms")
        print(f"  2-hop average: {hop2_avg:.2f}ms")
        print(f"  2-hop is {hop2_avg/hop1_avg:.1f}x slower than 1-hop")
    
    print("\n" + "="*70)
    print()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Benchmark 1-hop and 2-hop graph traversals')
    parser.add_argument('--graph-name', type=str, default=GRAPH_NAME,
                       help='Name of the graph (default: generated_graph)')
    parser.add_argument('--iterations', type=int, default=10,
                       help='Number of iterations per query (default: 10)')
    parser.add_argument('--num-nodes', type=int, default=5,
                       help='Number of random nodes to test (default: 5)')
    parser.add_argument('--node-id', type=int, default=None,
                       help='Test specific node ID instead of random nodes')
    
    args = parser.parse_args()
    
    if args.node_id:
        # Test specific node
        print("="*70)
        print(f"HOP TRAVERSAL BENCHMARK - Node {args.node_id}")
        print("="*70)
        
        print("\n1-HOP DIRECTED:")
        result = benchmark_1_hop(args.graph_name, args.node_id, args.iterations)
        print(f"  Results: {result['result_count']} nodes")
        print(f"  Min: {result['min']:.2f}ms")
        print(f"  Max: {result['max']:.2f}ms")
        print(f"  Mean: {result['mean']:.2f}ms")
        print(f"  Median: {result['median']:.2f}ms")
        print(f"  Std Dev: {result['stdev']:.2f}ms")
        
        print("\n2-HOP DIRECTED:")
        result = benchmark_2_hop(args.graph_name, args.node_id, args.iterations)
        print(f"  Results: {result['result_count']} nodes")
        print(f"  Min: {result['min']:.2f}ms")
        print(f"  Max: {result['max']:.2f}ms")
        print(f"  Mean: {result['mean']:.2f}ms")
        print(f"  Median: {result['median']:.2f}ms")
        print(f"  Std Dev: {result['stdev']:.2f}ms")
        
        print("\n1-HOP UNDIRECTED:")
        result = benchmark_undirected_1_hop(args.graph_name, args.node_id, args.iterations)
        print(f"  Results: {result['result_count']} nodes")
        print(f"  Min: {result['min']:.2f}ms")
        print(f"  Max: {result['max']:.2f}ms")
        print(f"  Mean: {result['mean']:.2f}ms")
        print(f"  Median: {result['median']:.2f}ms")
        print(f"  Std Dev: {result['stdev']:.2f}ms")
        
        print("\n2-HOP UNDIRECTED:")
        result = benchmark_undirected_2_hop(args.graph_name, args.node_id, args.iterations)
        print(f"  Results: {result['result_count']} nodes")
        print(f"  Min: {result['min']:.2f}ms")
        print(f"  Max: {result['max']:.2f}ms")
        print(f"  Mean: {result['mean']:.2f}ms")
        print(f"  Median: {result['median']:.2f}ms")
        print(f"  Std Dev: {result['stdev']:.2f}ms")
        
        print()
    else:
        # Run full benchmark suite
        run_benchmark_suite(args.graph_name, args.iterations, args.num_nodes)

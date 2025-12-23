# benchmark_queries.py
import time
import statistics
from db_connection import get_connection
from config import GRAPH_NAME

def run_timed_query(query, graph_name=GRAPH_NAME):
    """
    Execute a Cypher query and return execution time in milliseconds.
    
    Returns:
        tuple: (execution_time_ms, results)
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        full_query = f"""
        SELECT * FROM cypher('{graph_name}', $$
            {query}
        $$) as (result agtype);
        """
        
        # Start timing
        start_time = time.time()
        
        # Execute query
        cur.execute(full_query)
        results = cur.fetchall()
        
        # End timing
        end_time = time.time()
        
        execution_time_ms = (end_time - start_time) * 1000
        
        return execution_time_ms, results
        
    finally:
        cur.close()
        conn.close()

def benchmark_query(query, query_name, iterations=10, graph_name=GRAPH_NAME):
    """
    Benchmark a query by running it multiple times.
    
    Args:
        query: Cypher query string
        query_name: Descriptive name for the query
        iterations: Number of times to run the query
        graph_name: Name of the graph
    
    Returns:
        dict: Statistics about query performance
    """
    print(f"\n{'='*70}")
    print(f"Benchmarking: {query_name}")
    print(f"{'='*70}")
    print(f"Iterations: {iterations}")
    print(f"Graph: {graph_name}")
    print(f"\nQuery:")
    print(f"{query}")
    print(f"\n{'─'*70}")
    
    times = []
    result_count = None
    
    for i in range(iterations):
        try:
            exec_time, results = run_timed_query(query, graph_name)
            times.append(exec_time)
            
            if result_count is None:
                result_count = len(results)
            
            print(f"Run {i+1:2d}: {exec_time:8.2f} ms")
            
        except Exception as e:
            print(f"Run {i+1:2d}: ERROR - {e}")
    
    if not times:
        print("\n✗ All runs failed!")
        return None
    
    # Calculate statistics
    stats = {
        'query_name': query_name,
        'iterations': len(times),
        'result_count': result_count,
        'min_ms': min(times),
        'max_ms': max(times),
        'mean_ms': statistics.mean(times),
        'median_ms': statistics.median(times),
        'stdev_ms': statistics.stdev(times) if len(times) > 1 else 0,
        'times': times
    }
    
    # Print summary
    print(f"\n{'─'*70}")
    print(f"Results:")
    print(f"  Results returned: {result_count}")
    print(f"  Min time:         {stats['min_ms']:.2f} ms")
    print(f"  Max time:         {stats['max_ms']:.2f} ms")
    print(f"  Mean time:        {stats['mean_ms']:.2f} ms")
    print(f"  Median time:      {stats['median_ms']:.2f} ms")
    print(f"  Std deviation:    {stats['stdev_ms']:.2f} ms")
    print(f"{'='*70}")
    
    return stats

def find_nodes_within_distance_2(node_id, iterations=10, graph_name=GRAPH_NAME):
    """
    Benchmark query to find all nodes within distance 2 of a given node.
    
    Args:
        node_id: ID of the starting node
        iterations: Number of times to run the query
        graph_name: Name of the graph
    
    Returns:
        dict: Query performance statistics
    """
    query = f"""
        MATCH (start)
        WHERE start.id = {node_id}
        MATCH (start)-[*1..2]-(connected)
        RETURN DISTINCT connected.id, connected.name, labels(connected)[0] as type
    """
    
    query_name = f"Find Nodes Within Distance 2 (from node {node_id})"
    
    return benchmark_query(query, query_name, iterations, graph_name)

def get_sample_node_ids(graph_name=GRAPH_NAME, count=5):
    """
    Get a sample of node IDs from the graph.
    
    Args:
        graph_name: Name of the graph
        count: Number of sample IDs to retrieve
    
    Returns:
        list: List of node IDs
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        query = f"""
        SELECT * FROM cypher('{graph_name}', $$
            MATCH (n)
            RETURN n.id
            LIMIT {count}
        $$) as (id agtype);
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        # Extract IDs from agtype results
        ids = [int(str(row[0])) for row in results]
        
        return ids
        
    finally:
        cur.close()
        conn.close()

def benchmark_multiple_nodes(node_ids=None, iterations=10, graph_name=GRAPH_NAME):
    """
    Benchmark the distance-2 query for multiple starting nodes.
    
    Args:
        node_ids: List of node IDs to test (if None, will sample from graph)
        iterations: Number of iterations per node
        graph_name: Name of the graph
    """
    if node_ids is None:
        print("Sampling node IDs from graph...")
        node_ids = get_sample_node_ids(graph_name, count=5)
        print(f"Testing with nodes: {node_ids}\n")
    
    all_stats = []
    
    for node_id in node_ids:
        stats = find_nodes_within_distance_2(node_id, iterations, graph_name)
        if stats:
            all_stats.append(stats)
    
    # Print overall summary
    print(f"\n{'='*70}")
    print("OVERALL SUMMARY")
    print(f"{'='*70}")
    print(f"Tested {len(all_stats)} different starting nodes")
    print(f"\nAverage performance across all nodes:")
    
    if all_stats:
        overall_mean = statistics.mean([s['mean_ms'] for s in all_stats])
        overall_median = statistics.median([s['median_ms'] for s in all_stats])
        overall_min = min([s['min_ms'] for s in all_stats])
        overall_max = max([s['max_ms'] for s in all_stats])
        
        print(f"  Overall min:      {overall_min:.2f} ms")
        print(f"  Overall max:      {overall_max:.2f} ms")
        print(f"  Overall mean:     {overall_mean:.2f} ms")
        print(f"  Overall median:   {overall_median:.2f} ms")
    
    print(f"{'='*70}\n")
    
    return all_stats

# Example usage and main function
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Benchmark AGE graph queries')
    parser.add_argument('--node-id', type=int, help='Specific node ID to test')
    parser.add_argument('--iterations', type=int, default=10, help='Number of iterations per query')
    parser.add_argument('--graph-name', type=str, default=GRAPH_NAME, help='Name of the graph')
    parser.add_argument('--multiple', action='store_true', help='Test multiple random nodes')
    parser.add_argument('--count', type=int, default=5, help='Number of nodes to test (with --multiple)')
    
    args = parser.parse_args()
    
    if args.node_id:
        # Test specific node
        find_nodes_within_distance_2(args.node_id, args.iterations, args.graph_name)
    elif args.multiple:
        # Test multiple random nodes
        benchmark_multiple_nodes(None, args.iterations, args.graph_name)
    else:
        # Default: test with first available node
        print("Getting sample node ID...")
        node_ids = get_sample_node_ids(args.graph_name, count=1)
        if node_ids:
            print(f"Testing with node ID: {node_ids[0]}\n")
            find_nodes_within_distance_2(node_ids[0], args.iterations, args.graph_name)
        else:
            print("No nodes found in graph!")

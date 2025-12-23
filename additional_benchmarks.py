# additional_benchmarks.py
"""
Additional benchmark queries for AGE graph database.
Extends benchmark_queries.py with more query patterns.
"""

from benchmark_queries import benchmark_query, get_sample_node_ids
from config import GRAPH_NAME

def benchmark_distance_1(node_id, iterations=10, graph_name=GRAPH_NAME):
    """Find all immediate neighbors (distance 1)."""
    query = f"""
        MATCH (start)
        WHERE start.id = {node_id}
        MATCH (start)--(neighbor)
        RETURN DISTINCT neighbor.id, neighbor.name, labels(neighbor)[0] as type
    """
    
    return benchmark_query(
        query, 
        f"Find Immediate Neighbors (distance 1 from node {node_id})",
        iterations,
        graph_name
    )

def benchmark_distance_3(node_id, iterations=10, graph_name=GRAPH_NAME):
    """Find all nodes within distance 3."""
    query = f"""
        MATCH (start)
        WHERE start.id = {node_id}
        MATCH (start)-[*1..3]-(connected)
        RETURN DISTINCT connected.id, connected.name, labels(connected)[0] as type
    """
    
    return benchmark_query(
        query,
        f"Find Nodes Within Distance 3 (from node {node_id})",
        iterations,
        graph_name
    )

def benchmark_shortest_path(node_id_1, node_id_2, iterations=10, graph_name=GRAPH_NAME):
    """Find shortest path between two nodes."""
    query = f"""
        MATCH (start), (end)
        WHERE start.id = {node_id_1} AND end.id = {node_id_2}
        MATCH path = shortestPath((start)-[*]-(end))
        RETURN path, length(path) as path_length
    """
    
    return benchmark_query(
        query,
        f"Shortest Path (node {node_id_1} to node {node_id_2})",
        iterations,
        graph_name
    )

def benchmark_count_by_type(iterations=10, graph_name=GRAPH_NAME):
    """Count nodes by type."""
    query = """
        MATCH (n)
        RETURN labels(n)[0] as type, count(n) as count
        ORDER BY count DESC
    """
    
    return benchmark_query(
        query,
        "Count Nodes by Type",
        iterations,
        graph_name
    )

def benchmark_find_person_works_at(iterations=10, graph_name=GRAPH_NAME):
    """Find all person-company work relationships."""
    query = """
        MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
        RETURN p.name, r.position, c.name, c.industry
        LIMIT 100
    """
    
    return benchmark_query(
        query,
        "Find Person-Company Work Relationships",
        iterations,
        graph_name
    )

def benchmark_complex_pattern(iterations=10, graph_name=GRAPH_NAME):
    """Complex pattern: Person who purchased products and works at company."""
    query = """
        MATCH (p:Person)-[w:WORKS_AT]->(c:Company),
              (p)-[pur:PURCHASED]->(prod:Product)
        RETURN p.name, c.name, collect(prod.name) as products, count(prod) as product_count
        ORDER BY product_count DESC
        LIMIT 50
    """
    
    return benchmark_query(
        query,
        "Complex Pattern - Person, Company, Products",
        iterations,
        graph_name
    )

def benchmark_aggregation(iterations=10, graph_name=GRAPH_NAME):
    """Aggregation query: Average properties by group."""
    query = """
        MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
        RETURN c.industry, 
               count(p) as employees,
               avg(p.age) as avg_age,
               avg(r.salary) as avg_salary
        ORDER BY employees DESC
    """
    
    return benchmark_query(
        query,
        "Aggregation - Company Statistics",
        iterations,
        graph_name
    )

def benchmark_filtered_traversal(node_id, min_age=30, iterations=10, graph_name=GRAPH_NAME):
    """Traversal with filtering on properties."""
    query = f"""
        MATCH (start)
        WHERE start.id = {node_id}
        MATCH (start)-[*1..2]-(p:Person)
        WHERE p.age >= {min_age}
        RETURN DISTINCT p.id, p.name, p.age
        ORDER BY p.age DESC
    """
    
    return benchmark_query(
        query,
        f"Filtered Traversal - Find People age >= {min_age} within distance 2",
        iterations,
        graph_name
    )

def run_comprehensive_benchmark(graph_name=GRAPH_NAME, iterations=10):
    """Run a comprehensive suite of benchmarks."""
    print("\n" + "="*70)
    print("COMPREHENSIVE BENCHMARK SUITE")
    print("="*70)
    
    results = {}
    
    # Get sample node IDs
    print("\nGetting sample node IDs...")
    node_ids = get_sample_node_ids(graph_name, count=3)
    print(f"Testing with nodes: {node_ids}\n")
    
    if len(node_ids) < 1:
        print("Error: No nodes found in graph!")
        return results
    
    # 1. Distance queries
    print("\n" + "─"*70)
    print("DISTANCE-BASED QUERIES")
    print("─"*70)
    
    results['distance_1'] = benchmark_distance_1(node_ids[0], iterations, graph_name)
    results['distance_2'] = benchmark_query(
        f"""
        MATCH (start)
        WHERE start.id = {node_ids[0]}
        MATCH (start)-[*1..2]-(connected)
        RETURN DISTINCT connected.id, connected.name, labels(connected)[0] as type
        """,
        f"Find Nodes Within Distance 2 (from node {node_ids[0]})",
        iterations,
        graph_name
    )
    results['distance_3'] = benchmark_distance_3(node_ids[0], iterations, graph_name)
    
    # 2. Shortest path
    if len(node_ids) >= 2:
        print("\n" + "─"*70)
        print("PATH QUERIES")
        print("─"*70)
        results['shortest_path'] = benchmark_shortest_path(
            node_ids[0], node_ids[1], iterations, graph_name
        )
    
    # 3. Aggregation queries
    print("\n" + "─"*70)
    print("AGGREGATION QUERIES")
    print("─"*70)
    
    results['count_by_type'] = benchmark_count_by_type(iterations, graph_name)
    results['aggregation'] = benchmark_aggregation(iterations, graph_name)
    
    # 4. Pattern matching
    print("\n" + "─"*70)
    print("PATTERN MATCHING QUERIES")
    print("─"*70)
    
    results['works_at'] = benchmark_find_person_works_at(iterations, graph_name)
    results['complex_pattern'] = benchmark_complex_pattern(iterations, graph_name)
    
    # 5. Filtered traversal
    print("\n" + "─"*70)
    print("FILTERED QUERIES")
    print("─"*70)
    
    results['filtered_traversal'] = benchmark_filtered_traversal(
        node_ids[0], min_age=30, iterations=iterations, graph_name=graph_name
    )
    
    # Print summary
    print("\n" + "="*70)
    print("BENCHMARK SUMMARY")
    print("="*70)
    
    print(f"\n{'Query':<40} {'Mean (ms)':<12} {'Median (ms)':<12} {'Results'}")
    print("─"*70)
    
    for name, stats in results.items():
        if stats:
            print(f"{stats['query_name'][:38]:<40} {stats['mean_ms']:<12.2f} {stats['median_ms']:<12.2f} {stats['result_count']}")
    
    print("="*70 + "\n")
    
    return results

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run additional benchmark queries')
    parser.add_argument('--comprehensive', action='store_true', 
                       help='Run comprehensive benchmark suite')
    parser.add_argument('--iterations', type=int, default=10, 
                       help='Number of iterations per query')
    parser.add_argument('--graph-name', type=str, default=GRAPH_NAME, 
                       help='Name of the graph')
    
    args = parser.parse_args()
    
    if args.comprehensive:
        run_comprehensive_benchmark(args.graph_name, args.iterations)
    else:
        print("Use --comprehensive to run the full benchmark suite")
        print("Or use benchmark_queries.py for individual distance-2 queries")

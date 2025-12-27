# performance_comparison.py
"""
Compare loading performance between AGLoad and Cypher methods.
"""

import time
import pandas as pd
from generate_nodes import generate_nodes, person_properties, company_properties, product_properties, location_properties
from generate_edges import generate_edges, works_at_properties, purchased_properties, knows_properties, located_in_properties
from load_to_age import load_nodes_to_age, load_edges_to_age
from agload_loader import load_with_agload_from_dataframes
from db_connection import setup_age_environment, create_graph
from delete_graph import delete_all_data
from config import GRAPH_NAME

def run_performance_test(num_persons=1000, num_companies=100, num_products=500, 
                        num_locations=50, density=0.05):
    """
    Run performance comparison between AGLoad and Cypher loading.
    """
    
    print("="*70)
    print("PERFORMANCE COMPARISON: AGLoad vs Cypher")
    print("="*70)
    print(f"\nTest Configuration:")
    print(f"  Persons: {num_persons:,}")
    print(f"  Companies: {num_companies:,}")
    print(f"  Products: {num_products:,}")
    print(f"  Locations: {num_locations:,}")
    print(f"  Edge density: {density:.2%}")
    
    # Generate data once
    print("\n" + "─"*70)
    print("Generating test data...")
    print("─"*70)
    
    node_types = {
        'Person': person_properties,
        'Company': company_properties,
        'Product': product_properties,
        'Location': location_properties
    }
    
    num_nodes = {
        'Person': num_persons,
        'Company': num_companies,
        'Product': num_products,
        'Location': num_locations
    }
    
    nodes_df = generate_nodes(node_types, num_nodes)
    
    edge_types = {
        'WORKS_AT': ('Person', 'Company', works_at_properties),
        'PURCHASED': ('Person', 'Product', purchased_properties),
        'KNOWS': ('Person', 'Person', knows_properties),
        'LOCATED_IN': ('Company', 'Location', located_in_properties),
    }
    
    edges_df = generate_edges(nodes_df, edge_types, density)
    
    total_nodes = len(nodes_df)
    total_edges = len(edges_df)
    total_items = total_nodes + total_edges
    
    print(f"\nGenerated {total_nodes:,} nodes and {total_edges:,} edges")
    print(f"Total items: {total_items:,}")
    
    # Setup graph
    graph_name = "perf_test_graph"
    setup_age_environment()
    
    results = {}
    
    # Test 1: AGLoad
    print("\n" + "="*70)
    print("TEST 1: AGLoad Bulk Loading")
    print("="*70)
    
    try:
        # Create graph for AGLoad test
        create_graph(graph_name)
        
        start_time = time.time()
        stats = load_with_agload_from_dataframes(nodes_df, edges_df, graph_name, cleanup=True)
        agload_time = time.time() - start_time
        
        if stats:
            results['agload'] = {
                'time': agload_time,
                'nodes': total_nodes,
                'edges': total_edges,
                'total_items': total_items,
                'rate': total_items / agload_time
            }
            print(f"\n✓ AGLoad completed in {agload_time:.2f} seconds")
            print(f"  Rate: {total_items / agload_time:.1f} items/sec")
        else:
            print("\n✗ AGLoad not available or failed")
            results['agload'] = None
        
        # Clean up for next test
        delete_all_data(graph_name, batch_size=5000, confirm=False)
        
    except Exception as e:
        print(f"\n✗ AGLoad test failed: {e}")
        results['agload'] = None
    
    # Test 2: Cypher with batch_size=100
    print("\n" + "="*70)
    print("TEST 2: Cypher Loading (batch_size=100)")
    print("="*70)
    
    try:
        # Create graph for Cypher test
        create_graph(graph_name)
        
        start_time = time.time()
        load_nodes_to_age(nodes_df, graph_name, batch_size=100)
        load_edges_to_age(edges_df, graph_name, batch_size=100)
        cypher_100_time = time.time() - start_time
        
        results['cypher_100'] = {
            'time': cypher_100_time,
            'nodes': total_nodes,
            'edges': total_edges,
            'total_items': total_items,
            'rate': total_items / cypher_100_time
        }
        print(f"\n✓ Cypher (batch=100) completed in {cypher_100_time:.2f} seconds")
        print(f"  Rate: {total_items / cypher_100_time:.1f} items/sec")
        
        # Clean up for next test
        delete_all_data(graph_name, batch_size=5000, confirm=False)
        
    except Exception as e:
        print(f"\n✗ Cypher test failed: {e}")
        results['cypher_100'] = None
    
    # Test 3: Cypher with batch_size=500
    print("\n" + "="*70)
    print("TEST 3: Cypher Loading (batch_size=500)")
    print("="*70)
    
    try:
        # Create graph for Cypher test
        create_graph(graph_name)
        
        start_time = time.time()
        load_nodes_to_age(nodes_df, graph_name, batch_size=500)
        load_edges_to_age(edges_df, graph_name, batch_size=500)
        cypher_500_time = time.time() - start_time
        
        results['cypher_500'] = {
            'time': cypher_500_time,
            'nodes': total_nodes,
            'edges': total_edges,
            'total_items': total_items,
            'rate': total_items / cypher_500_time
        }
        print(f"\n✓ Cypher (batch=500) completed in {cypher_500_time:.2f} seconds")
        print(f"  Rate: {total_items / cypher_500_time:.1f} items/sec")
        
        # Final cleanup
        delete_all_data(graph_name, batch_size=5000, confirm=False)
        
    except Exception as e:
        print(f"\n✗ Cypher test failed: {e}")
        results['cypher_500'] = None
    
    # Print comparison
    print("\n" + "="*70)
    print("PERFORMANCE COMPARISON RESULTS")
    print("="*70)
    print(f"\nDataset: {total_nodes:,} nodes, {total_edges:,} edges ({total_items:,} total)")
    print("\n" + "─"*70)
    print(f"{'Method':<30} {'Time (s)':<15} {'Rate (items/s)':<20} {'Speedup'}")
    print("─"*70)
    
    baseline_time = None
    if results.get('cypher_100'):
        baseline_time = results['cypher_100']['time']
    
    for method_name, method_label in [
        ('agload', 'AGLoad (bulk)'),
        ('cypher_100', 'Cypher (batch=100)'),
        ('cypher_500', 'Cypher (batch=500)')
    ]:
        if results.get(method_name):
            r = results[method_name]
            speedup = f"{baseline_time / r['time']:.2f}x" if baseline_time else "baseline"
            print(f"{method_label:<30} {r['time']:<15.2f} {r['rate']:<20.1f} {speedup}")
        else:
            print(f"{method_label:<30} {'N/A':<15} {'N/A':<20} {'N/A'}")
    
    print("─"*70)
    
    # Recommendations
    print("\nRecommendations:")
    if results.get('agload'):
        agload_speedup = baseline_time / results['agload']['time'] if baseline_time else 0
        if agload_speedup > 5:
            print(f"  ✓ AGLoad is {agload_speedup:.1f}x faster - USE IT for large datasets!")
        elif agload_speedup > 2:
            print(f"  ✓ AGLoad is {agload_speedup:.1f}x faster - recommended for better performance")
        else:
            print(f"  • AGLoad is {agload_speedup:.1f}x faster - marginal improvement")
    else:
        print("  ⚠ AGLoad not available - install it for better performance")
    
    if results.get('cypher_500') and results.get('cypher_100'):
        if results['cypher_500']['time'] < results['cypher_100']['time']:
            improvement = (results['cypher_100']['time'] / results['cypher_500']['time'] - 1) * 100
            print(f"  • For Cypher loading, larger batches (500) are {improvement:.1f}% faster")
    
    print("\n" + "="*70 + "\n")
    
    return results

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Compare AGLoad vs Cypher performance')
    parser.add_argument('--persons', type=int, default=5000, 
                       help='Number of Person nodes (default: 5000)')
    parser.add_argument('--companies', type=int, default=500, 
                       help='Number of Company nodes (default: 500)')
    parser.add_argument('--products', type=int, default=2000, 
                       help='Number of Product nodes (default: 2000)')
    parser.add_argument('--locations', type=int, default=200, 
                       help='Number of Location nodes (default: 200)')
    parser.add_argument('--density', type=float, default=0.05, 
                       help='Edge density (default: 0.05)')
    
    args = parser.parse_args()
    
    run_performance_test(
        num_persons=args.persons,
        num_companies=args.companies,
        num_products=args.products,
        num_locations=args.locations,
        density=args.density
    )

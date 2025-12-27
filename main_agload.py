# main_agload.py
"""
Main script for generating and loading graph data into Apache AGE using AGLoad.
AGLoad is significantly faster than Cypher-based loading.
"""

import argparse
from generate_nodes import generate_nodes, person_properties, company_properties, product_properties, location_properties
from generate_edges import generate_edges, works_at_properties, purchased_properties, knows_properties, located_in_properties
from agload_loader import load_with_agload_from_dataframes
from db_connection import setup_age_environment, create_graph
from load_to_age import create_indexes
from config import GRAPH_NAME

def main():
    parser = argparse.ArgumentParser(description='Generate and load graph data into Apache AGE using AGLoad')
    parser.add_argument('--persons', type=int, default=100, help='Number of Person nodes')
    parser.add_argument('--companies', type=int, default=20, help='Number of Company nodes')
    parser.add_argument('--products', type=int, default=50, help='Number of Product nodes')
    parser.add_argument('--locations', type=int, default=10, help='Number of Location nodes')
    parser.add_argument('--density', type=float, default=0.05, help='Edge density (0.0 to 1.0)')
    parser.add_argument('--graph-name', type=str, default=GRAPH_NAME, help='Name of the graph')
    parser.add_argument('--keep-csv', action='store_true', help='Keep temporary CSV files after loading')
    
    args = parser.parse_args()
    
    print("="*70)
    print("Graph Data Generation and Loading (AGLoad)")
    print("="*70)
    
    # Step 1: Generate nodes
    print("\n[1/5] Generating nodes...")
    node_types = {
        'Person': person_properties,
        'Company': company_properties,
        'Product': product_properties,
        'Location': location_properties
    }
    
    num_nodes = {
        'Person': args.persons,
        'Company': args.companies,
        'Product': args.products,
        'Location': args.locations
    }
    
    nodes_df = generate_nodes(node_types, num_nodes)
    nodes_df.to_csv('nodes.csv', index=False)
    print(f"   Generated {len(nodes_df):,} nodes")
    
    # Step 2: Generate edges
    print("\n[2/5] Generating edges...")
    edge_types = {
        'WORKS_AT': ('Person', 'Company', works_at_properties),
        'PURCHASED': ('Person', 'Product', purchased_properties),
        'KNOWS': ('Person', 'Person', knows_properties),
        'LOCATED_IN': ('Company', 'Location', located_in_properties),
    }
    
    edges_df = generate_edges(nodes_df, edge_types, args.density)
    edges_df.to_csv('edges.csv', index=False)
    print(f"   Generated {len(edges_df):,} edges")
    
    # Step 3: Setup AGE
    print("\n[3/5] Setting up AGE environment...")
    setup_age_environment()
    create_graph(args.graph_name)
    
    # Step 4: Load data with AGLoad
    print("\n[4/5] Loading data with AGLoad...")
    stats = load_with_agload_from_dataframes(
        nodes_df, 
        edges_df, 
        args.graph_name,
        cleanup=not args.keep_csv
    )
    
    if not stats:
        print("\n" + "="*70)
        print("✗ AGLoad failed or is not installed")
        print("="*70)
        print("\nTo install AGLoad, run:")
        print("  ./install_agload.sh")
        print("\nOr use the Cypher-based loader:")
        print("  python main.py")
        print("="*70 + "\n")
        return
    
    # Step 5: Create indexes
    print("\n[5/5] Creating indexes...")
    create_indexes(args.graph_name)
    
    print("\n" + "="*70)
    print("✓ Graph generation and loading complete!")
    print("="*70)
    print(f"\nGraph name: {args.graph_name}")
    print(f"Total nodes: {len(nodes_df):,}")
    print(f"Total edges: {len(edges_df):,}")
    print(f"Edge density: {args.density:.2%}")
    print(f"Loading method: AGLoad (bulk)")
    print(f"Loading time: {stats['time_taken']:.2f} seconds")
    print(f"Loading rate: {stats['rate']:.1f} items/sec")
    
    # Print node breakdown
    print(f"\nNode breakdown:")
    for label in num_nodes:
        count = len(nodes_df[nodes_df['label'] == label])
        print(f"  {label}: {count:,}")
    
    # Print edge breakdown
    print(f"\nEdge breakdown:")
    for edge_label in edge_types:
        count = len(edges_df[edges_df['edge_label'] == edge_label])
        print(f"  {edge_label}: {count:,}")
    
    print()

if __name__ == "__main__":
    main()

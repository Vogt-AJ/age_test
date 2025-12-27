# main.py
import argparse
from generate_nodes import generate_nodes, person_properties, company_properties, product_properties, location_properties
from generate_edges import generate_edges, works_at_properties, purchased_properties, knows_properties, located_in_properties
from ultra_fast_cypher_loader import ultra_fast_load_nodes, ultra_fast_load_edges
from load_to_age import create_indexes
from db_connection import setup_age_environment, create_graph
from config import GRAPH_NAME

def main():
    parser = argparse.ArgumentParser(description='Generate and load graph data into Apache AGE')
    parser.add_argument('--persons', type=int, default=100, help='Number of Person nodes')
    parser.add_argument('--companies', type=int, default=20, help='Number of Company nodes')
    parser.add_argument('--products', type=int, default=50, help='Number of Product nodes')
    parser.add_argument('--locations', type=int, default=10, help='Number of Location nodes')
    parser.add_argument('--density', type=float, default=0.05, help='Edge density (0.0 to 1.0)')
    parser.add_argument('--graph-name', type=str, default=GRAPH_NAME, help='Name of the graph')
    parser.add_argument('--batch-size', type=int, default=5000, help='Batch size for loading (default: 5000, try 10000+ for large datasets)')
    
    args = parser.parse_args()
    
    print("="*60)
    print("Graph Data Generation and Loading")
    print("="*60)
    
    # Step 1: Generate nodes
    print("\n[1/6] Generating nodes...")
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
    print(f"   Generated {len(nodes_df)} nodes")
    
    # Step 2: Generate edges
    print("\n[2/6] Generating edges...")
    edge_types = {
        'WORKS_AT': ('Person', 'Company', works_at_properties),
        'PURCHASED': ('Person', 'Product', purchased_properties),
        'KNOWS': ('Person', 'Person', knows_properties),
        'LOCATED_IN': ('Company', 'Location', located_in_properties),
    }
    
    edges_df = generate_edges(nodes_df, edge_types, args.density)
    edges_df.to_csv('edges.csv', index=False)
    print(f"   Generated {len(edges_df)} edges")
    
    # Step 3: Setup AGE
    print("\n[3/6] Setting up AGE environment...")
    setup_age_environment()
    create_graph(args.graph_name)
    
    # Step 4: Load nodes
    print("\n[4/6] Loading nodes into AGE (Ultra-Fast method)...")
    ultra_fast_load_nodes(nodes_df, args.graph_name, args.batch_size)
    
    # Step 5: Load edges
    print("\n[5/6] Loading edges into AGE (Ultra-Fast method)...")
    ultra_fast_load_edges(edges_df, args.graph_name, args.batch_size)
    
    # Create indexes
    print("\n[6/6] Creating indexes...")
    create_indexes(args.graph_name)
    
    print("\n" + "="*70)
    print("✓ Graph generation and loading complete!")
    print("="*70)
    print(f"\nGraph name: {args.graph_name}")
    print(f"Total nodes: {len(nodes_df):,}")
    print(f"Total edges: {len(edges_df):,}")
    print(f"Edge density: {args.density:.2%}")
    print(f"Loading method: Ultra-Fast Cypher (batch size: {args.batch_size:,})")
    
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
    
    print(f"\nTip: For large datasets (1M+ edges), use --batch-size 10000 or higher")
    print()

if __name__ == "__main__":
    main()

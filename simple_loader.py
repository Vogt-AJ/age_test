# simple_loader.py
"""
Simple, reliable loader using the proven batched method.
"""

import pandas as pd
from load_to_age import load_nodes_to_age, load_edges_to_age, create_indexes
from db_connection import setup_age_environment, create_graph
from config import GRAPH_NAME

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Load graph data using proven batched method')
    parser.add_argument('--graph-name', type=str, default=GRAPH_NAME,
                       help='Name of the graph')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Batch size (default: 100)')
    
    args = parser.parse_args()
    
    # Setup
    print("\nSetting up AGE environment...")
    setup_age_environment()
    create_graph(args.graph_name)
    
    # Load data
    print("\nReading CSV files...")
    nodes_df = pd.read_csv('nodes.csv')
    edges_df = pd.read_csv('edges.csv')
    
    print(f"Loaded {len(nodes_df):,} nodes and {len(edges_df):,} edges from CSV\n")
    
    # Load with proven method
    print(f"Using batch size: {args.batch_size}\n")
    load_nodes_to_age(nodes_df, args.graph_name, args.batch_size)
    load_edges_to_age(edges_df, args.graph_name, args.batch_size)
    create_indexes(args.graph_name)
    
    print("\n✓ Loading complete!\n")

if __name__ == "__main__":
    main()

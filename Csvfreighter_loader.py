# csvfreighter_loader.py
"""
Bulk loading using CSVFreighter from agefreighter package.
Uses async API to load data from CSV files.
"""

import asyncio
import os
import sys
import pandas as pd
from agefreighter import Factory
from config import DB_CONFIG, GRAPH_NAME

async def load_with_csvfreighter(graph_name=GRAPH_NAME, drop_graph=False):
    """
    Load graph data using CSVFreighter.
    
    Note: CSVFreighter expects CSV files with specific format:
    - Each CSV contains: start vertex, edge, end vertex data
    - We need to prepare CSVs in this format
    """
    
    print("="*70)
    print("CSVFREIGHTER BULK LOADING")
    print("="*70)
    
    # Create PostgreSQL DSN (Data Source Name)
    dsn = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    
    print(f"\n✓ Using database: {DB_CONFIG['database']}")
    print(f"✓ Graph: {graph_name}")
    
    # Create CSVFreighter instance
    instance = Factory.create_instance("CSVFreighter")
    
    # Connect to database
    print("\nConnecting to database...")
    await instance.connect(
        dsn=dsn,
        max_connections=16,
        min_connections=4,
    )
    print("✓ Connected")
    
    # Prepare combined CSV files for CSVFreighter
    # CSVFreighter expects: start_vertex_props, edge_props, end_vertex_props in same CSV
    print("\n" + "─"*70)
    print("Preparing CSV files for CSVFreighter...")
    print("─"*70)
    
    # Load our generated data
    nodes_df = pd.read_csv('nodes.csv')
    edges_df = pd.read_csv('edges.csv')
    
    print(f"Found {len(nodes_df):,} nodes and {len(edges_df):,} edges")
    
    # Group edges by type and create combined CSVs
    for edge_label in edges_df['edge_label'].unique():
        label_edges = edges_df[edges_df['edge_label'] == edge_label].copy()
        
        # Get first edge to determine vertex types
        first_edge = label_edges.iloc[0]
        
        # Find source and target vertex labels
        from_node = nodes_df[nodes_df['id'] == first_edge['from_id']].iloc[0]
        to_node = nodes_df[nodes_df['id'] == first_edge['to_id']].iloc[0]
        
        start_label = from_node['label']
        end_label = to_node['label']
        
        print(f"\nPreparing CSV for: {start_label} -[{edge_label}]-> {end_label}")
        
        # Build combined CSV
        combined_rows = []
        
        for _, edge in label_edges.iterrows():
            # Get start vertex
            start_vertex = nodes_df[nodes_df['id'] == edge['from_id']].iloc[0]
            start_props = eval(start_vertex['properties']) if isinstance(start_vertex['properties'], str) else start_vertex['properties']
            
            # Get end vertex
            end_vertex = nodes_df[nodes_df['id'] == edge['to_id']].iloc[0]
            end_props = eval(end_vertex['properties']) if isinstance(end_vertex['properties'], str) else end_vertex['properties']
            
            # Get edge props
            edge_props = eval(edge['properties']) if isinstance(edge['properties'], str) else edge['properties']
            
            # Combine into one row
            row = {}
            
            # Add start vertex properties with prefix
            row['start_id'] = edge['from_id']
            for k, v in start_props.items():
                row[f'start_{k}'] = v
            
            # Add edge properties
            for k, v in edge_props.items():
                row[f'edge_{k}'] = v
            
            # Add end vertex properties with prefix
            row['end_id'] = edge['to_id']
            for k, v in end_props.items():
                row[f'end_{k}'] = v
            
            combined_rows.append(row)
        
        # Create DataFrame and save
        combined_df = pd.DataFrame(combined_rows)
        csv_filename = f'csvfreighter_{start_label}_{edge_label}_{end_label}.csv'
        combined_df.to_csv(csv_filename, index=False)
        
        print(f"  Created {csv_filename} with {len(combined_df):,} rows")
        
        # Get property column names
        start_prop_cols = [col for col in combined_df.columns if col.startswith('start_') and col != 'start_id']
        edge_prop_cols = [col for col in combined_df.columns if col.startswith('edge_')]
        end_prop_cols = [col for col in combined_df.columns if col.startswith('end_') and col != 'end_id']
        
        # Strip prefixes for property names
        start_props = [col.replace('start_', '') for col in start_prop_cols]
        edge_props_list = [col.replace('edge_', '') for col in edge_prop_cols]
        end_props = [col.replace('end_', '') for col in end_prop_cols]
        
        # Load using CSVFreighter
        print(f"\n  Loading {csv_filename}...")
        
        await instance.load(
            graph_name=graph_name,
            start_v_label=start_label,
            start_id='start_id',
            start_props=start_props,
            edge_type=edge_label,
            edge_props=edge_props_list,
            end_v_label=end_label,
            end_id='end_id',
            end_props=end_props,
            csv_path=csv_filename,
            use_copy=True,
            drop_graph=drop_graph,  # Only drop on first iteration
            create_graph=(drop_graph or edge_label == edges_df['edge_label'].unique()[0]),  # Create if first
            progress=True,
        )
        
        # After first load, don't drop or create graph anymore
        drop_graph = False
        
        print(f"  ✓ Loaded {edge_label} edges")
    
    print("\n" + "="*70)
    print("LOADING COMPLETE")
    print("="*70)
    print(f"Total nodes: {len(nodes_df):,}")
    print(f"Total edges: {len(edges_df):,}")
    print("="*70 + "\n")

async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Load graph data using CSVFreighter')
    parser.add_argument('--graph-name', type=str, default=GRAPH_NAME,
                       help='Name of the graph')
    parser.add_argument('--drop-graph', action='store_true',
                       help='Drop existing graph before loading')
    
    args = parser.parse_args()
    
    # Check if CSV files exist
    if not os.path.exists('nodes.csv'):
        print("Error: nodes.csv not found!")
        print("Generate data first with generate_nodes.py")
        return
    
    if not os.path.exists('edges.csv'):
        print("Error: edges.csv not found!")
        print("Generate data first with generate_edges.py")
        return
    
    await load_with_csvfreighter(args.graph_name, args.drop_graph)
    
    print("\n✓ Success! Run 'python quick_check.py' to verify.")

if __name__ == "__main__":
    # Windows-specific event loop policy
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
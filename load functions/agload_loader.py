# agload_loader.py
"""
Bulk loading for Apache AGE using the AGLoad utility.
AGLoad is significantly faster than Cypher-based loading for large datasets.
"""

import pandas as pd
import json
import subprocess
import os
import time
from config import DB_CONFIG, GRAPH_NAME

def prepare_vertex_csv(nodes_df, output_file='vertices.csv'):
    """
    Prepare vertex CSV file in AGLoad format.
    
    AGLoad vertex format:
    - First column: vertex id
    - Remaining columns: properties (or single JSON column)
    
    Args:
        nodes_df: DataFrame with columns: id, label, properties
        output_file: Output CSV filename
    
    Returns:
        dict: Mapping of label to CSV file path
    """
    print(f"\n{'='*70}")
    print(f"Preparing vertex CSV files for AGLoad")
    print(f"{'='*70}\n")
    
    vertex_files = {}
    
    # Group nodes by label
    for label in nodes_df['label'].unique():
        label_df = nodes_df[nodes_df['label'] == label].copy()
        
        # Prepare the data
        rows = []
        for _, row in label_df.iterrows():
            # Parse properties
            if isinstance(row['properties'], str):
                properties = eval(row['properties'])
            else:
                properties = row['properties']
            
            # Create row with id and all properties as separate columns
            row_data = {'id': row['id']}
            row_data.update(properties)
            rows.append(row_data)
        
        # Create DataFrame
        df = pd.DataFrame(rows)
        
        # Save to CSV
        filename = f"vertices_{label}.csv"
        df.to_csv(filename, index=False)
        vertex_files[label] = filename
        
        print(f"✓ Created {filename}: {len(df):,} vertices")
    
    print(f"\n{'─'*70}\n")
    return vertex_files

def prepare_edge_csv(edges_df, output_file='edges.csv'):
    """
    Prepare edge CSV file in AGLoad format.
    
    AGLoad edge format:
    - start_id: ID of source vertex
    - end_id: ID of target vertex  
    - Remaining columns: properties
    
    Args:
        edges_df: DataFrame with columns: from_id, to_id, edge_label, properties
        output_file: Output CSV filename
    
    Returns:
        dict: Mapping of edge label to CSV file path
    """
    print(f"\n{'='*70}")
    print(f"Preparing edge CSV files for AGLoad")
    print(f"{'='*70}\n")
    
    edge_files = {}
    
    # Group edges by label
    for edge_label in edges_df['edge_label'].unique():
        label_df = edges_df[edges_df['edge_label'] == edge_label].copy()
        
        # Prepare the data
        rows = []
        for _, row in label_df.iterrows():
            # Parse properties
            if isinstance(row['properties'], str):
                properties = eval(row['properties'])
            else:
                properties = row['properties']
            
            # Create row with start_id, end_id, and properties
            row_data = {
                'start_id': row['from_id'],
                'end_id': row['to_id']
            }
            row_data.update(properties)
            rows.append(row_data)
        
        # Create DataFrame
        df = pd.DataFrame(rows)
        
        # Save to CSV
        filename = f"edges_{edge_label}.csv"
        df.to_csv(filename, index=False)
        edge_files[edge_label] = filename
        
        print(f"✓ Created {filename}: {len(df):,} edges")
    
    print(f"\n{'─'*70}\n")
    return edge_files

def load_with_agload(graph_name=GRAPH_NAME, vertex_files=None, edge_files=None):
    """
    Load data using AGLoad bulk loader.
    
    Args:
        graph_name: Name of the graph
        vertex_files: Dict of {label: csv_file}
        edge_files: Dict of {edge_label: csv_file}
    
    Returns:
        dict: Loading statistics
    """
    print(f"\n{'='*70}")
    print(f"Loading graph data using AGLoad")
    print(f"{'='*70}\n")
    
    start_time = time.time()
    total_vertices = 0
    total_edges = 0
    
    # Build connection string
    conn_string = (
        f"host={DB_CONFIG['host']} "
        f"port={DB_CONFIG['port']} "
        f"dbname={DB_CONFIG['database']} "
        f"user={DB_CONFIG['user']} "
        f"password={DB_CONFIG['password']}"
    )
    
    try:
        # Load vertices
        if vertex_files:
            print("Loading vertices...")
            print("─" * 70)
            
            for label, csv_file in vertex_files.items():
                print(f"\nLoading {label} vertices from {csv_file}...")
                
                cmd = [
                    'age_load',
                    '--dbname', DB_CONFIG['database'],
                    '--host', DB_CONFIG['host'],
                    '--port', str(DB_CONFIG['port']),
                    '--username', DB_CONFIG['user'],
                    '--password', DB_CONFIG['password'],
                    '--graph', graph_name,
                    '--label', label,
                    '--type', 'vertex',
                    '--csv-path', csv_file
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    # Count lines in CSV (excluding header)
                    with open(csv_file, 'r') as f:
                        count = sum(1 for _ in f) - 1
                    total_vertices += count
                    print(f"  ✓ Loaded {count:,} {label} vertices")
                else:
                    print(f"  ✗ Error loading {label} vertices:")
                    print(f"    {result.stderr}")
            
            print(f"\n{'─'*70}")
            print(f"Total vertices loaded: {total_vertices:,}")
        
        # Load edges
        if edge_files:
            print(f"\n{'─'*70}")
            print("Loading edges...")
            print("─" * 70)
            
            for edge_label, csv_file in edge_files.items():
                print(f"\nLoading {edge_label} edges from {csv_file}...")
                
                cmd = [
                    'age_load',
                    '--dbname', DB_CONFIG['database'],
                    '--host', DB_CONFIG['host'],
                    '--port', str(DB_CONFIG['port']),
                    '--username', DB_CONFIG['user'],
                    '--password', DB_CONFIG['password'],
                    '--graph', graph_name,
                    '--label', edge_label,
                    '--type', 'edge',
                    '--csv-path', csv_file
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    # Count lines in CSV (excluding header)
                    with open(csv_file, 'r') as f:
                        count = sum(1 for _ in f) - 1
                    total_edges += count
                    print(f"  ✓ Loaded {count:,} {edge_label} edges")
                else:
                    print(f"  ✗ Error loading {edge_label} edges:")
                    print(f"    {result.stderr}")
        
        elapsed_time = time.time() - start_time
        
        print(f"\n{'='*70}")
        print(f"✓ AGLoad completed in {elapsed_time:.2f} seconds")
        print(f"{'='*70}")
        print(f"Total vertices: {total_vertices:,}")
        print(f"Total edges: {total_edges:,}")
        print(f"Average rate: {(total_vertices + total_edges) / elapsed_time:.1f} items/sec")
        print(f"{'='*70}\n")
        
        return {
            'vertices_loaded': total_vertices,
            'edges_loaded': total_edges,
            'time_taken': elapsed_time,
            'rate': (total_vertices + total_edges) / elapsed_time
        }
        
    except FileNotFoundError:
        print("\n✗ Error: 'age_load' command not found!")
        print("AGLoad may not be installed or not in your PATH.")
        print("\nTo install AGLoad:")
        print("  cd /path/to/age/tools")
        print("  make install")
        print("\nFalling back to Cypher-based loading...")
        return None
    except Exception as e:
        print(f"\n✗ Error during AGLoad: {e}")
        raise

def load_with_agload_from_dataframes(nodes_df, edges_df, graph_name=GRAPH_NAME, cleanup=True):
    """
    Complete pipeline: prepare CSVs and load using AGLoad.
    
    Args:
        nodes_df: DataFrame with node data
        edges_df: DataFrame with edge data
        graph_name: Name of the graph
        cleanup: Whether to delete CSV files after loading
    
    Returns:
        dict: Loading statistics
    """
    # Prepare CSV files
    vertex_files = prepare_vertex_csv(nodes_df)
    edge_files = prepare_edge_csv(edges_df)
    
    # Load with AGLoad
    stats = load_with_agload(graph_name, vertex_files, edge_files)
    
    # Cleanup CSV files
    if cleanup and stats:
        print("\nCleaning up temporary CSV files...")
        for csv_file in list(vertex_files.values()) + list(edge_files.values()):
            try:
                os.remove(csv_file)
                print(f"  Removed {csv_file}")
            except Exception as e:
                print(f"  Warning: Could not remove {csv_file}: {e}")
    
    return stats

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Load graph data using AGLoad')
    parser.add_argument('--graph-name', type=str, default=GRAPH_NAME,
                       help='Name of the graph')
    parser.add_argument('--no-cleanup', action='store_true',
                       help='Keep CSV files after loading')
    
    args = parser.parse_args()
    
    # Load data
    print("Reading CSV files...")
    nodes_df = pd.read_csv('nodes.csv')
    edges_df = pd.read_csv('edges.csv')
    
    print(f"Loaded {len(nodes_df):,} nodes and {len(edges_df):,} edges from CSV\n")
    
    # Load using AGLoad
    stats = load_with_agload_from_dataframes(
        nodes_df, 
        edges_df, 
        args.graph_name,
        cleanup=not args.no_cleanup
    )
    
    if stats:
        print("\n" + "="*70)
        print("SUCCESS!")
        print("="*70)
    else:
        print("\n" + "="*70)
        print("AGLoad failed - consider using Cypher-based loading instead")
        print("Run: python load_to_age.py")
        print("="*70)

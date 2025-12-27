# ultra_fast_cypher_loader.py
"""
Ultra-optimized Cypher loader with massive batching for large datasets.
Uses single multi-statement queries to minimize overhead.
"""

import pandas as pd
import time
from db_connection import get_connection
from config import GRAPH_NAME
from load_to_age import create_indexes

def ultra_fast_load_nodes(nodes_df, graph_name=GRAPH_NAME, batch_size=5000):
    """
    Load nodes with massive batching - creates many nodes in single query.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    total_nodes = len(nodes_df)
    loaded_count = 0
    start_time = time.time()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        print(f"\n{'='*70}")
        print(f"Ultra-fast loading {total_nodes:,} nodes")
        print(f"Batch size: {batch_size:,}")
        print(f"{'='*70}\n")
        
        for label in nodes_df['label'].unique():
            label_df = nodes_df[nodes_df['label'] == label]
            print(f"Loading {len(label_df):,} {label} nodes...")
            
            # Process in batches
            for i in range(0, len(label_df), batch_size):
                batch = label_df.iloc[i:i+batch_size]
                
                # Build multi-CREATE query
                creates = []
                for _, row in batch.iterrows():
                    if isinstance(row['properties'], str):
                        properties = eval(row['properties'])
                    else:
                        properties = row['properties']
                    
                    properties['id'] = row['id']
                    props_str = ", ".join([f"{k}: '{v}'" if isinstance(v, str) else f"{k}: {v}" 
                                           for k, v in properties.items()])
                    
                    creates.append(f"CREATE (:{label} {{{props_str}}})")
                
                # Execute all creates in one query
                cypher_query = f"""
                SELECT * FROM cypher('{graph_name}', $$
                    {' '.join(creates)}
                $$) as (v agtype);
                """
                
                cur.execute(cypher_query)
                conn.commit()
                
                loaded_count += len(batch)
                
                elapsed = time.time() - start_time
                progress_pct = (loaded_count / total_nodes) * 100
                rate = loaded_count / elapsed if elapsed > 0 else 0
                eta = (total_nodes - loaded_count) / rate if rate > 0 else 0
                
                print(f"  Progress: {loaded_count:,}/{total_nodes:,} ({progress_pct:.1f}%) | "
                      f"Rate: {rate:.1f} nodes/sec | ETA: {eta:.1f}s")
        
        elapsed_time = time.time() - start_time
        print(f"\n{'─'*70}")
        print(f"✓ Loaded {loaded_count:,} nodes in {elapsed_time:.2f} seconds")
        print(f"  Average rate: {loaded_count/elapsed_time:.1f} nodes/sec")
        print(f"{'─'*70}\n")
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def ultra_fast_load_edges(edges_df, graph_name=GRAPH_NAME, batch_size=5000):
    """
    Load edges with massive batching - creates many edges in single query.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    total_edges = len(edges_df)
    loaded_count = 0
    start_time = time.time()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        print(f"\n{'='*70}")
        print(f"Ultra-fast loading {total_edges:,} edges")
        print(f"Batch size: {batch_size:,}")
        print(f"{'='*70}\n")
        
        for edge_label in edges_df['edge_label'].unique():
            label_df = edges_df[edges_df['edge_label'] == edge_label]
            print(f"Loading {len(label_df):,} {edge_label} edges...")
            
            # Process in batches
            for i in range(0, len(label_df), batch_size):
                batch = label_df.iloc[i:i+batch_size]
                
                # Build multi-MATCH-CREATE query
                statements = []
                for _, row in batch.iterrows():
                    if isinstance(row['properties'], str):
                        properties = eval(row['properties'])
                    else:
                        properties = row['properties']
                    
                    if properties:
                        props_str = ", ".join([f"{k}: '{v}'" if isinstance(v, str) else f"{k}: {v}" 
                                               for k, v in properties.items()])
                        props_clause = f"{{{props_str}}}"
                    else:
                        props_clause = ""
                    
                    statements.append(
                        f"MATCH (a {{id: {row['from_id']}}}), (b {{id: {row['to_id']}}}) "
                        f"CREATE (a)-[:{edge_label} {props_clause}]->(b)"
                    )
                
                # Execute all in one query
                cypher_query = f"""
                SELECT * FROM cypher('{graph_name}', $$
                    {' '.join(statements)}
                $$) as (v agtype);
                """
                
                try:
                    cur.execute(cypher_query)
                    conn.commit()
                    loaded_count += len(batch)
                except Exception as e:
                    print(f"  Warning: Batch {i//batch_size + 1} had errors: {e}")
                    conn.rollback()
                    continue
                
                elapsed = time.time() - start_time
                progress_pct = (loaded_count / total_edges) * 100
                rate = loaded_count / elapsed if elapsed > 0 else 0
                eta = (total_edges - loaded_count) / rate if rate > 0 else 0
                
                print(f"  Progress: {loaded_count:,}/{total_edges:,} ({progress_pct:.1f}%) | "
                      f"Rate: {rate:.1f} edges/sec | ETA: {eta:.1f}s")
        
        elapsed_time = time.time() - start_time
        print(f"\n{'─'*70}")
        print(f"✓ Loaded {loaded_count:,} edges in {elapsed_time:.2f} seconds")
        print(f"  Average rate: {loaded_count/elapsed_time:.1f} edges/sec")
        print(f"{'─'*70}\n")
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    import argparse
    from db_connection import setup_age_environment, create_graph
    
    parser = argparse.ArgumentParser(description='Ultra-fast Cypher loading with massive batches')
    parser.add_argument('--graph-name', type=str, default=GRAPH_NAME,
                       help='Name of the graph')
    parser.add_argument('--batch-size', type=int, default=5000,
                       help='Batch size (default: 5000, try 10000+ for huge datasets)')
    
    args = parser.parse_args()
    
    # Setup
    setup_age_environment()
    create_graph(args.graph_name)
    
    # Load data
    print("Reading CSV files...")
    nodes_df = pd.read_csv('nodes.csv')
    edges_df = pd.read_csv('edges.csv')
    
    print(f"Loaded {len(nodes_df):,} nodes and {len(edges_df):,} edges from CSV\n")
    
    # Ultra-fast load
    ultra_fast_load_nodes(nodes_df, args.graph_name, args.batch_size)
    ultra_fast_load_edges(edges_df, args.graph_name, args.batch_size)
    create_indexes(args.graph_name)
    
    print("\n✓ Ultra-fast loading complete!\n")

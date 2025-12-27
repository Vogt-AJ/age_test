# load_to_age.py
import pandas as pd
import json
from db_connection import get_connection, create_graph, setup_age_environment
from config import GRAPH_NAME

def load_nodes_to_age(nodes_df, graph_name=GRAPH_NAME, batch_size=100):
    """
    Load nodes from DataFrame into AGE graph with batching and progress updates.
    
    Args:
        nodes_df: DataFrame containing node data
        graph_name: Name of the graph
        batch_size: Number of nodes to commit in each batch
    """
    import time
    
    conn = get_connection()
    cur = conn.cursor()
    
    total_nodes = len(nodes_df)
    loaded_count = 0
    start_time = time.time()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        print(f"\n{'='*70}")
        print(f"Loading {total_nodes} nodes into graph '{graph_name}'")
        print(f"Batch size: {batch_size}")
        print(f"{'='*70}\n")
        
        for idx, row in nodes_df.iterrows():
            node_id = row['id']
            label = row['label']
            
            # Parse properties from string if needed
            if isinstance(row['properties'], str):
                properties = eval(row['properties'])
            else:
                properties = row['properties']
            
            # Convert properties to AGE format
            props_str = ", ".join([f"{k}: '{v}'" if isinstance(v, str) else f"{k}: {v}" 
                                   for k, v in properties.items()])
            
            # Create Cypher query
            cypher_query = f"""
            SELECT * FROM cypher('{graph_name}', $$
                CREATE (n:{label} {{id: {node_id}, {props_str}}})
                RETURN n
            $$) as (v agtype);
            """
            
            cur.execute(cypher_query)
            loaded_count += 1
            
            # Commit in batches
            if loaded_count % batch_size == 0:
                conn.commit()
                elapsed_time = time.time() - start_time
                progress_pct = (loaded_count / total_nodes) * 100
                rate = loaded_count / elapsed_time
                eta = (total_nodes - loaded_count) / rate if rate > 0 else 0
                
                print(f"Progress: {loaded_count:,}/{total_nodes:,} ({progress_pct:.1f}%) | "
                      f"Rate: {rate:.1f} nodes/sec | "
                      f"ETA: {eta:.1f}s")
        
        # Final commit for remaining nodes
        conn.commit()
        
        elapsed_time = time.time() - start_time
        print(f"\n{'─'*70}")
        print(f"✓ Loaded {loaded_count:,} nodes in {elapsed_time:.2f} seconds")
        print(f"  Average rate: {loaded_count/elapsed_time:.1f} nodes/sec")
        print(f"{'─'*70}\n")
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error loading nodes at node {loaded_count + 1}: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def load_edges_to_age(edges_df, graph_name=GRAPH_NAME, batch_size=100):
    """
    Load edges from DataFrame into AGE graph with batching and progress updates.
    
    Args:
        edges_df: DataFrame containing edge data
        graph_name: Name of the graph
        batch_size: Number of edges to commit in each batch
    """
    import time
    
    conn = get_connection()
    cur = conn.cursor()
    
    total_edges = len(edges_df)
    loaded_count = 0
    skipped_count = 0
    start_time = time.time()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        print(f"\n{'='*70}")
        print(f"Loading {total_edges} edges into graph '{graph_name}'")
        print(f"Batch size: {batch_size}")
        print(f"{'='*70}\n")
        
        for idx, row in edges_df.iterrows():
            from_id = row['from_id']
            to_id = row['to_id']
            edge_label = row['edge_label']
            
            # Parse properties from string if needed
            if isinstance(row['properties'], str):
                properties = eval(row['properties'])
            else:
                properties = row['properties']
            
            # Convert properties to AGE format
            if properties:
                props_str = ", ".join([f"{k}: '{v}'" if isinstance(v, str) else f"{k}: {v}" 
                                       for k, v in properties.items()])
                props_clause = f"{{{props_str}}}"
            else:
                props_clause = ""
            
            # Create Cypher query
            cypher_query = f"""
            SELECT * FROM cypher('{graph_name}', $$
                MATCH (a), (b)
                WHERE a.id = {from_id} AND b.id = {to_id}
                CREATE (a)-[r:{edge_label} {props_clause}]->(b)
                RETURN r
            $$) as (v agtype);
            """
            
            try:
                cur.execute(cypher_query)
                loaded_count += 1
            except Exception as edge_error:
                skipped_count += 1
                if skipped_count <= 5:  # Only print first few errors
                    print(f"Warning: Skipped edge {from_id}->{to_id}: {edge_error}")
            
            processed_count = loaded_count + skipped_count
            
            # Commit in batches
            if processed_count % batch_size == 0:
                conn.commit()
                elapsed_time = time.time() - start_time
                progress_pct = (processed_count / total_edges) * 100
                rate = processed_count / elapsed_time
                eta = (total_edges - processed_count) / rate if rate > 0 else 0
                
                print(f"Progress: {processed_count:,}/{total_edges:,} ({progress_pct:.1f}%) | "
                      f"Loaded: {loaded_count:,} | Skipped: {skipped_count:,} | "
                      f"Rate: {rate:.1f} edges/sec | "
                      f"ETA: {eta:.1f}s")
        
        # Final commit for remaining edges
        conn.commit()
        
        elapsed_time = time.time() - start_time
        print(f"\n{'─'*70}")
        print(f"✓ Loaded {loaded_count:,} edges in {elapsed_time:.2f} seconds")
        if skipped_count > 0:
            print(f"  ⚠ Skipped {skipped_count:,} edges (nodes not found or other errors)")
        print(f"  Average rate: {loaded_count/elapsed_time:.1f} edges/sec")
        print(f"{'─'*70}\n")
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error loading edges: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def create_indexes(graph_name=GRAPH_NAME):
    """Create indexes on node IDs for better query performance."""
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        print(f"\n{'='*70}")
        print(f"Creating indexes for graph '{graph_name}'")
        print(f"{'='*70}\n")
        
        # Get all vertex labels
        cur.execute(f"""
            SELECT name FROM ag_catalog.ag_label 
            WHERE graph = (SELECT graphid FROM ag_catalog.ag_graph WHERE name = '{graph_name}')
            AND kind = 'v';
        """)
        
        labels = [row[0] for row in cur.fetchall()]
        
        if not labels:
            print("No vertex labels found. Skipping index creation.")
            return
        
        print(f"Found {len(labels)} vertex labels: {', '.join(labels)}\n")
        
        created_count = 0
        skipped_count = 0
        
        for i, label in enumerate(labels, 1):
            try:
                # Create index on id property
                index_query = f"""
                CREATE INDEX IF NOT EXISTS {label}_id_idx 
                ON {graph_name}."{label}" ((properties->'id'));
                """
                cur.execute(index_query)
                created_count += 1
                print(f"[{i}/{len(labels)}] ✓ Created index for {label}.id")
            except Exception as e:
                skipped_count += 1
                print(f"[{i}/{len(labels)}] ⚠ Index for {label}.id: {str(e)[:60]}")
        
        conn.commit()
        
        print(f"\n{'─'*70}")
        print(f"Index creation complete:")
        print(f"  Created: {created_count}")
        print(f"  Skipped: {skipped_count}")
        print(f"{'─'*70}\n")
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error creating indexes: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Load graph data into Apache AGE')
    parser.add_argument('--batch-size', type=int, default=100, 
                       help='Number of nodes/edges to commit in each batch (default: 100)')
    parser.add_argument('--graph-name', type=str, default=GRAPH_NAME,
                       help='Name of the graph')
    
    args = parser.parse_args()
    
    # Setup
    setup_age_environment()
    create_graph(args.graph_name)
    
    # Load data
    nodes_df = pd.read_csv('nodes.csv')
    edges_df = pd.read_csv('edges.csv')
    
    load_nodes_to_age(nodes_df, args.graph_name, args.batch_size)
    load_edges_to_age(edges_df, args.graph_name, args.batch_size)
    create_indexes(args.graph_name)
    
    print("\n" + "="*70)
    print("✓ Data loading complete!")
    print("="*70)

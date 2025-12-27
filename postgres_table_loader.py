# postgres_table_loader.py
"""
Ultra-fast loading: PostgreSQL tables → Graph
1. Load data into temp PostgreSQL tables using COPY (millions of rows/sec)
2. Build graph from tables using Cypher (set-based operations)
This is typically the fastest approach for very large datasets.
"""

import pandas as pd
import time
import io
from db_connection import get_connection
from config import GRAPH_NAME
from load_to_age import create_indexes

def load_nodes_via_postgres_tables(nodes_df, graph_name=GRAPH_NAME):
    """
    Load nodes by first inserting into PostgreSQL tables, then creating graph nodes.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    total_nodes = len(nodes_df)
    start_time = time.time()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        print(f"\n{'='*70}")
        print(f"Loading {total_nodes:,} nodes via PostgreSQL tables")
        print(f"{'='*70}\n")
        
        # Step 1: Create temp table and load data using COPY (very fast)
        print("Step 1: Loading data into PostgreSQL temp table...")
        
        cur.execute("""
            CREATE TEMP TABLE temp_nodes (
                node_id BIGINT,
                label TEXT,
                properties JSONB
            );
        """)
        
        # Prepare data for COPY
        buffer = io.StringIO()
        for _, row in nodes_df.iterrows():
            if isinstance(row['properties'], str):
                properties = eval(row['properties'])
            else:
                properties = row['properties']
            
            properties['id'] = row['id']
            
            import json
            props_json = json.dumps(properties)
            
            buffer.write(f"{row['id']}\t{row['label']}\t{props_json}\n")
        
        buffer.seek(0)
        
        # Use COPY - this is VERY fast
        copy_start = time.time()
        cur.copy_expert(
            "COPY temp_nodes (node_id, label, properties) FROM STDIN",
            buffer
        )
        copy_time = time.time() - copy_start
        
        print(f"  ✓ Loaded {total_nodes:,} rows into temp table in {copy_time:.2f}s")
        print(f"  Rate: {total_nodes/copy_time:.1f} rows/sec")
        
        # Step 2: Create graph nodes from temp table (set-based operation)
        print("\nStep 2: Creating graph nodes from table...")
        
        # Get unique labels
        cur.execute("SELECT DISTINCT label FROM temp_nodes;")
        labels = [row[0] for row in cur.fetchall()]
        
        for label in labels:
            label_start = time.time()
            
            # Count nodes for this label
            cur.execute(f"SELECT COUNT(*) FROM temp_nodes WHERE label = '{label}';")
            count = cur.fetchone()[0]
            
            print(f"\n  Creating {count:,} {label} nodes...")
            
            # Create all nodes for this label in batches
            batch_size = 5000
            cur.execute(f"SELECT COUNT(*) FROM temp_nodes WHERE label = '{label}';")
            total_for_label = cur.fetchone()[0]
            
            for offset in range(0, total_for_label, batch_size):
                # Get batch of rows
                cur.execute(f"""
                    SELECT properties 
                    FROM temp_nodes 
                    WHERE label = '{label}'
                    LIMIT {batch_size} OFFSET {offset};
                """)
                
                rows = cur.fetchall()
                
                # Build multi-CREATE statement
                creates = []
                for row in rows:
                    props = row[0]  # JSONB automatically parsed
                    
                    # Convert JSONB to Cypher properties format
                    props_list = []
                    for key, value in props.items():
                        if isinstance(value, str):
                            props_list.append(f"{key}: '{value}'")
                        elif isinstance(value, bool):
                            props_list.append(f"{key}: {str(value).lower()}")
                        else:
                            props_list.append(f"{key}: {value}")
                    
                    props_str = ", ".join(props_list)
                    creates.append(f"CREATE (:{label} {{{props_str}}})")
                
                # Execute batch
                if creates:
                    cypher_query = f"""
                    SELECT * FROM cypher('{graph_name}', $$
                        {' '.join(creates)}
                    $$) as (v agtype);
                    """
                    cur.execute(cypher_query)
                    conn.commit()
                
                progress = min(offset + batch_size, total_for_label)
                print(f"    Progress: {progress:,}/{total_for_label:,} ({progress/total_for_label*100:.1f}%)")
            
            label_time = time.time() - label_start
            print(f"  ✓ Created {count:,} {label} nodes in {label_time:.2f}s")
        
        # Clean up temp table
        cur.execute("DROP TABLE temp_nodes;")
        conn.commit()
        
        elapsed_time = time.time() - start_time
        print(f"\n{'─'*70}")
        print(f"✓ Total nodes created: {total_nodes:,}")
        print(f"  Total time: {elapsed_time:.2f} seconds")
        print(f"  Overall rate: {total_nodes/elapsed_time:.1f} nodes/sec")
        print(f"{'─'*70}\n")
        
        return {
            'nodes_loaded': total_nodes,
            'time_taken': elapsed_time,
            'rate': total_nodes / elapsed_time
        }
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        cur.close()
        conn.close()

def load_edges_via_postgres_tables(edges_df, graph_name=GRAPH_NAME):
    """
    Load edges by first inserting into PostgreSQL tables, then creating graph edges.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    total_edges = len(edges_df)
    start_time = time.time()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        print(f"\n{'='*70}")
        print(f"Loading {total_edges:,} edges via PostgreSQL tables")
        print(f"{'='*70}\n")
        
        # Step 1: Create temp table and load data using COPY
        print("Step 1: Loading data into PostgreSQL temp table...")
        
        cur.execute("""
            CREATE TEMP TABLE temp_edges (
                from_id BIGINT,
                to_id BIGINT,
                edge_label TEXT,
                properties JSONB
            );
        """)
        
        # Prepare data for COPY
        buffer = io.StringIO()
        for _, row in edges_df.iterrows():
            if isinstance(row['properties'], str):
                properties = eval(row['properties'])
            else:
                properties = row['properties']
            
            import json
            props_json = json.dumps(properties) if properties else '{}'
            
            buffer.write(f"{row['from_id']}\t{row['to_id']}\t{row['edge_label']}\t{props_json}\n")
        
        buffer.seek(0)
        
        # Use COPY
        copy_start = time.time()
        cur.copy_expert(
            "COPY temp_edges (from_id, to_id, edge_label, properties) FROM STDIN",
            buffer
        )
        copy_time = time.time() - copy_start
        
        print(f"  ✓ Loaded {total_edges:,} rows into temp table in {copy_time:.2f}s")
        print(f"  Rate: {total_edges/copy_time:.1f} rows/sec")
        
        # Step 2: Create graph edges from temp table
        print("\nStep 2: Creating graph edges from table...")
        
        # Get unique edge labels
        cur.execute("SELECT DISTINCT edge_label FROM temp_edges;")
        edge_labels = [row[0] for row in cur.fetchall()]
        
        for edge_label in edge_labels:
            label_start = time.time()
            
            # Count edges for this label
            cur.execute(f"SELECT COUNT(*) FROM temp_edges WHERE edge_label = '{edge_label}';")
            count = cur.fetchone()[0]
            
            print(f"\n  Creating {count:,} {edge_label} edges...")
            
            # Create edges in batches
            batch_size = 5000
            cur.execute(f"SELECT COUNT(*) FROM temp_edges WHERE edge_label = '{edge_label}';")
            total_for_label = cur.fetchone()[0]
            
            loaded = 0
            for offset in range(0, total_for_label, batch_size):
                # Get batch of rows
                cur.execute(f"""
                    SELECT from_id, to_id, properties 
                    FROM temp_edges 
                    WHERE edge_label = '{edge_label}'
                    LIMIT {batch_size} OFFSET {offset};
                """)
                
                rows = cur.fetchall()
                
                # Build multi-MATCH-CREATE statement
                statements = []
                for row in rows:
                    from_id, to_id, props = row
                    
                    if props:
                        props_list = []
                        for key, value in props.items():
                            if isinstance(value, str):
                                props_list.append(f"{key}: '{value}'")
                            elif isinstance(value, bool):
                                props_list.append(f"{key}: {str(value).lower()}")
                            else:
                                props_list.append(f"{key}: {value}")
                        props_str = "{" + ", ".join(props_list) + "}"
                    else:
                        props_str = ""
                    
                    statements.append(
                        f"MATCH (a {{id: {from_id}}}), (b {{id: {to_id}}}) "
                        f"CREATE (a)-[:{edge_label} {props_str}]->(b)"
                    )
                
                # Execute batch
                if statements:
                    cypher_query = f"""
                    SELECT * FROM cypher('{graph_name}', $$
                        {' '.join(statements)}
                    $$) as (v agtype);
                    """
                    try:
                        cur.execute(cypher_query)
                        conn.commit()
                        loaded += len(rows)
                    except Exception as e:
                        print(f"    Warning: Batch failed: {e}")
                        conn.rollback()
                
                progress = min(offset + batch_size, total_for_label)
                elapsed = time.time() - label_start
                rate = loaded / elapsed if elapsed > 0 else 0
                print(f"    Progress: {progress:,}/{total_for_label:,} ({progress/total_for_label*100:.1f}%) | Rate: {rate:.1f} edges/sec")
            
            label_time = time.time() - label_start
            print(f"  ✓ Created {loaded:,} {edge_label} edges in {label_time:.2f}s")
        
        # Clean up temp table
        cur.execute("DROP TABLE temp_edges;")
        conn.commit()
        
        elapsed_time = time.time() - start_time
        print(f"\n{'─'*70}")
        print(f"✓ Total edges created: {total_edges:,}")
        print(f"  Total time: {elapsed_time:.2f} seconds")
        print(f"  Overall rate: {total_edges/elapsed_time:.1f} edges/sec")
        print(f"{'─'*70}\n")
        
        return {
            'edges_loaded': total_edges,
            'time_taken': elapsed_time,
            'rate': total_edges / elapsed_time
        }
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        cur.close()
        conn.close()

def postgres_table_load(nodes_df, edges_df, graph_name=GRAPH_NAME):
    """Complete loading pipeline using PostgreSQL tables."""
    
    print("="*70)
    print("POSTGRES TABLE LOADING METHOD")
    print("="*70)
    
    # Load nodes
    node_stats = load_nodes_via_postgres_tables(nodes_df, graph_name)
    
    # Load edges
    edge_stats = load_edges_via_postgres_tables(edges_df, graph_name)
    
    # Create indexes
    print("\nCreating indexes...")
    create_indexes(graph_name)
    
    total_time = node_stats['time_taken'] + edge_stats['time_taken']
    total_items = node_stats['nodes_loaded'] + edge_stats['edges_loaded']
    
    print("\n" + "="*70)
    print("LOADING COMPLETE")
    print("="*70)
    print(f"Total nodes: {node_stats['nodes_loaded']:,}")
    print(f"Total edges: {edge_stats['edges_loaded']:,}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Overall rate: {total_items/total_time:.1f} items/sec")
    print("="*70 + "\n")
    
    return {
        'nodes': node_stats,
        'edges': edge_stats,
        'total_time': total_time,
        'total_items': total_items
    }

if __name__ == "__main__":
    import argparse
    from db_connection import setup_age_environment, create_graph
    
    parser = argparse.ArgumentParser(description='Load graph data via PostgreSQL tables')
    parser.add_argument('--graph-name', type=str, default=GRAPH_NAME,
                       help='Name of the graph')
    
    args = parser.parse_args()
    
    # Setup
    setup_age_environment()
    create_graph(args.graph_name)
    
    # Load data
    print("Reading CSV files...")
    nodes_df = pd.read_csv('nodes.csv')
    edges_df = pd.read_csv('edges.csv')
    
    print(f"Loaded {len(nodes_df):,} nodes and {len(edges_df):,} edges from CSV\n")
    
    # Load via PostgreSQL tables
    stats = postgres_table_load(nodes_df, edges_df, args.graph_name)

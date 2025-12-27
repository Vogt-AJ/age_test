# delete_graph.py
"""
Simple script to delete all data from an Apache AGE graph.
"""

import time
from db_connection import get_connection
from config import GRAPH_NAME

def delete_all_data(graph_name=GRAPH_NAME, batch_size=1000, confirm=True):
    """
    Delete all nodes and edges from a graph in batches.
    
    Args:
        graph_name: Name of the graph to delete data from
        batch_size: Number of nodes to delete per batch
        confirm: Whether to ask for confirmation before deleting
    
    Returns:
        dict: Statistics about the deletion
    """
    
    if confirm:
        print(f"\n{'='*70}")
        print(f"WARNING: This will delete ALL data from graph '{graph_name}'")
        print(f"{'='*70}\n")
        response = input("Are you sure you want to continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Deletion cancelled.")
            return None
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        # First, count how many nodes exist
        print(f"\nCounting nodes in graph '{graph_name}'...")
        cur.execute(f"""
            SELECT * FROM cypher('{graph_name}', $$
                MATCH (n) RETURN count(n)
            $$) as (count agtype);
        """)
        total_nodes = int(str(cur.fetchone()[0]))
        
        print(f"Found {total_nodes:,} nodes to delete\n")
        
        if total_nodes == 0:
            print("Graph is already empty.")
            return {'nodes_deleted': 0, 'time_taken': 0}
        
        # Delete all nodes in batches (edges are automatically deleted)
        print(f"{'='*70}")
        print(f"Deleting all data from graph '{graph_name}'")
        print(f"Batch size: {batch_size:,}")
        print(f"{'='*70}\n")
        
        deleted_count = 0
        start_time = time.time()
        batch_num = 0
        
        while True:
            batch_num += 1
            
            # Delete a batch of nodes
            delete_query = f"""
                SELECT * FROM cypher('{graph_name}', $$
                    MATCH (n)
                    WITH n LIMIT {batch_size}
                    DETACH DELETE n
                    RETURN count(*) as deleted
                $$) as (deleted agtype);
            """
            
            cur.execute(delete_query)
            result = cur.fetchone()
            batch_deleted = int(str(result[0])) if result and result[0] else 0
            
            if batch_deleted == 0:
                break
            
            deleted_count += batch_deleted
            conn.commit()
            
            # Calculate progress
            elapsed_time = time.time() - start_time
            progress_pct = (deleted_count / total_nodes) * 100
            rate = deleted_count / elapsed_time if elapsed_time > 0 else 0
            eta = (total_nodes - deleted_count) / rate if rate > 0 else 0
            
            print(f"Batch {batch_num}: Deleted {deleted_count:,}/{total_nodes:,} "
                  f"({progress_pct:.1f}%) | Rate: {rate:.1f} nodes/sec | ETA: {eta:.1f}s")
        
        elapsed_time = time.time() - start_time
        
        print(f"\n{'─'*70}")
        print(f"✓ Deleted {deleted_count:,} nodes in {elapsed_time:.2f} seconds")
        print(f"  Average rate: {deleted_count/elapsed_time:.1f} nodes/sec")
        print(f"{'─'*70}\n")
        
        return {
            'nodes_deleted': deleted_count,
            'time_taken': elapsed_time,
            'rate': deleted_count / elapsed_time if elapsed_time > 0 else 0
        }
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error during deletion: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def drop_graph(graph_name=GRAPH_NAME, confirm=True):
    """
    Completely drop a graph (removes schema and all data).
    
    Args:
        graph_name: Name of the graph to drop
        confirm: Whether to ask for confirmation before dropping
    """
    
    if confirm:
        print(f"\n{'='*70}")
        print(f"WARNING: This will PERMANENTLY DROP graph '{graph_name}'")
        print(f"The graph and all its data will be completely removed.")
        print(f"{'='*70}\n")
        response = input("Are you sure you want to continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Drop cancelled.")
            return False
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        print(f"\nDropping graph '{graph_name}'...")
        
        # Drop the graph (cascade deletes all data)
        cur.execute(f"SELECT drop_graph('{graph_name}', true);")
        conn.commit()
        
        print(f"✓ Graph '{graph_name}' has been dropped successfully\n")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error dropping graph: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def verify_empty(graph_name=GRAPH_NAME):
    """Verify that a graph is empty."""
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        # Count nodes
        cur.execute(f"""
            SELECT * FROM cypher('{graph_name}', $$
                MATCH (n) RETURN count(n)
            $$) as (count agtype);
        """)
        node_count = int(str(cur.fetchone()[0]))
        
        # Count edges
        cur.execute(f"""
            SELECT * FROM cypher('{graph_name}', $$
                MATCH ()-[r]->() RETURN count(r)
            $$) as (count agtype);
        """)
        edge_count = int(str(cur.fetchone()[0]))
        
        print(f"\nGraph '{graph_name}' status:")
        print(f"  Nodes: {node_count:,}")
        print(f"  Edges: {edge_count:,}")
        
        if node_count == 0 and edge_count == 0:
            print("  ✓ Graph is empty\n")
            return True
        else:
            print("  ⚠ Graph still contains data\n")
            return False
        
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Delete data from Apache AGE graph')
    parser.add_argument('--graph-name', type=str, default=GRAPH_NAME,
                       help='Name of the graph')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Number of nodes to delete per batch (default: 1000)')
    parser.add_argument('--drop', action='store_true',
                       help='Completely drop the graph (removes everything)')
    parser.add_argument('--no-confirm', action='store_true',
                       help='Skip confirmation prompt (use with caution!)')
    parser.add_argument('--verify', action='store_true',
                       help='Only verify if graph is empty (no deletion)')
    
    args = parser.parse_args()
    
    if args.verify:
        # Just verify the graph is empty
        verify_empty(args.graph_name)
    elif args.drop:
        # Drop the entire graph
        drop_graph(args.graph_name, confirm=not args.no_confirm)
    else:
        # Delete all data but keep the graph structure
        stats = delete_all_data(args.graph_name, args.batch_size, confirm=not args.no_confirm)
        
        if stats:
            # Verify deletion
            verify_empty(args.graph_name)

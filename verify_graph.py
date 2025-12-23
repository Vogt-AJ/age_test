# verify_graph.py
from db_connection import get_connection
from config import GRAPH_NAME
import json

def run_query(query, graph_name=GRAPH_NAME):
    """Execute a Cypher query and return results."""
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        full_query = f"""
        SELECT * FROM cypher('{graph_name}', $$
            {query}
        $$) as (result agtype);
        """
        
        cur.execute(full_query)
        results = cur.fetchall()
        return results
        
    finally:
        cur.close()
        conn.close()

def run_sql_query(query):
    """Execute a SQL query and return results."""
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute(query)
        results = cur.fetchall()
        return results
        
    finally:
        cur.close()
        conn.close()

def verify_graph(graph_name=GRAPH_NAME):
    """Perform comprehensive verification of the graph."""
    
    print("="*70)
    print(f"GRAPH VERIFICATION: {graph_name}")
    print("="*70)
    
    # 1. Check if graph exists
    print("\n[1] Checking if graph exists...")
    try:
        result = run_sql_query(f"SELECT * FROM ag_catalog.ag_graph WHERE name = '{graph_name}';")
        if result:
            print(f"   ✓ Graph '{graph_name}' exists")
            print(f"   Graph ID: {result[0][1]}")
        else:
            print(f"   ✗ Graph '{graph_name}' does NOT exist!")
            return
    except Exception as e:
        print(f"   ✗ Error checking graph: {e}")
        return
    
    # 2. Count total nodes and edges
    print("\n[2] Counting nodes and edges...")
    try:
        # Count nodes
        result = run_query("MATCH (n) RETURN count(n) as total")
        node_count = result[0][0] if result else 0
        print(f"   Total nodes: {node_count}")
        
        # Count edges
        result = run_query("MATCH ()-[r]->() RETURN count(r) as total")
        edge_count = result[0][0] if result else 0
        print(f"   Total edges: {edge_count}")
        
        if node_count == 0:
            print("   ✗ WARNING: No nodes found in graph!")
        else:
            print("   ✓ Graph contains data")
            
    except Exception as e:
        print(f"   ✗ Error counting: {e}")
    
    # 3. Check node labels (types)
    print("\n[3] Checking node labels...")
    try:
        result = run_query("MATCH (n) RETURN DISTINCT labels(n)[0] as label, count(n) as count ORDER BY label")
        if result:
            print("   Node type breakdown:")
            for row in result:
                # Parse the agtype result
                label_data = str(row[0])
                print(f"     • {label_data}")
        else:
            print("   ✗ No node labels found")
    except Exception as e:
        print(f"   ✗ Error checking labels: {e}")
    
    # 4. Check edge types
    print("\n[4] Checking edge types...")
    try:
        result = run_query("MATCH ()-[r]->() RETURN type(r) as edge_type, count(r) as count ORDER BY edge_type")
        if result:
            print("   Edge type breakdown:")
            for row in result:
                edge_data = str(row[0])
                print(f"     • {edge_data}")
        else:
            print("   ✗ No edges found")
    except Exception as e:
        print(f"   ✗ Error checking edges: {e}")
    
    # 5. Sample nodes from each type
    print("\n[5] Sampling nodes (first 2 of each type)...")
    try:
        node_types = ['Person', 'Company', 'Product', 'Location']
        for node_type in node_types:
            result = run_query(f"MATCH (n:{node_type}) RETURN n LIMIT 2")
            if result:
                print(f"\n   {node_type} samples:")
                for row in result:
                    print(f"     {row[0]}")
            else:
                print(f"   - No {node_type} nodes found")
    except Exception as e:
        print(f"   ✗ Error sampling nodes: {e}")
    
    # 6. Sample edges from each type
    print("\n[6] Sampling edges (first 2 of each type)...")
    try:
        edge_types = [
            ('WORKS_AT', 'Person', 'Company'),
            ('PURCHASED', 'Person', 'Product'),
            ('KNOWS', 'Person', 'Person'),
            ('LOCATED_IN', 'Company', 'Location')
        ]
        
        for edge_type, from_label, to_label in edge_types:
            query = f"""
                MATCH (a:{from_label})-[r:{edge_type}]->(b:{to_label})
                RETURN a.name, type(r), b.name, properties(r)
                LIMIT 2
            """
            result = run_query(query)
            if result:
                print(f"\n   {edge_type} samples:")
                for row in result:
                    print(f"     {row[0]}")
            else:
                print(f"   - No {edge_type} edges found")
    except Exception as e:
        print(f"   ✗ Error sampling edges: {e}")
    
    # 7. Check for orphaned nodes (nodes with no edges)
    print("\n[7] Checking for orphaned nodes...")
    try:
        result = run_query("""
            MATCH (n)
            WHERE NOT (n)-[]-()
            RETURN labels(n)[0] as label, count(n) as count
        """)
        if result and any(row[0] for row in result):
            print("   Orphaned nodes found:")
            for row in result:
                print(f"     • {row[0]}")
        else:
            print("   ✓ No orphaned nodes (all nodes have at least one edge)")
    except Exception as e:
        print(f"   Note: Could not check for orphaned nodes: {e}")
    
    # 8. Check indexes
    print("\n[8] Checking indexes...")
    try:
        result = run_sql_query(f"""
            SELECT indexname, tablename 
            FROM pg_indexes 
            WHERE schemaname = '{graph_name}'
            ORDER BY tablename;
        """)
        if result:
            print("   Indexes found:")
            for row in result:
                print(f"     • {row[0]} on {row[1]}")
        else:
            print("   ⚠ No indexes found (may affect query performance)")
    except Exception as e:
        print(f"   Note: Could not check indexes: {e}")
    
    # 9. Data integrity checks
    print("\n[9] Data integrity checks...")
    try:
        # Check for nodes with ID property
        result = run_query("MATCH (n) WHERE n.id IS NULL RETURN count(n)")
        null_id_count = result[0][0] if result else 0
        if null_id_count == 0:
            print("   ✓ All nodes have ID property")
        else:
            print(f"   ✗ Found {null_id_count} nodes without ID property")
        
        # Check for duplicate IDs
        result = run_query("""
            MATCH (n)
            WITH n.id as id, count(*) as count
            WHERE count > 1
            RETURN count(*)
        """)
        dup_count = result[0][0] if result else 0
        if dup_count == 0:
            print("   ✓ No duplicate IDs found")
        else:
            print(f"   ✗ Found {dup_count} duplicate IDs")
            
    except Exception as e:
        print(f"   Note: Could not perform integrity checks: {e}")
    
    # 10. Compare with CSV files if they exist
    print("\n[10] Comparing with source CSV files...")
    try:
        import pandas as pd
        import os
        
        if os.path.exists('nodes.csv') and os.path.exists('edges.csv'):
            nodes_df = pd.read_csv('nodes.csv')
            edges_df = pd.read_csv('edges.csv')
            
            csv_node_count = len(nodes_df)
            csv_edge_count = len(edges_df)
            
            print(f"   CSV nodes: {csv_node_count}")
            print(f"   Graph nodes: {node_count}")
            print(f"   CSV edges: {csv_edge_count}")
            print(f"   Graph edges: {edge_count}")
            
            if csv_node_count == node_count:
                print("   ✓ Node counts match!")
            else:
                print(f"   ✗ Node count mismatch! Missing {csv_node_count - node_count} nodes")
            
            if csv_edge_count == edge_count:
                print("   ✓ Edge counts match!")
            else:
                print(f"   ✗ Edge count mismatch! Missing {csv_edge_count - edge_count} edges")
        else:
            print("   - CSV files not found in current directory")
    except Exception as e:
        print(f"   Note: Could not compare with CSV files: {e}")
    
    # Summary
    print("\n" + "="*70)
    print("VERIFICATION COMPLETE")
    print("="*70)
    
    # Overall assessment
    if node_count > 0 and edge_count > 0:
        print("\n✓ Graph appears to be loaded correctly!")
        print(f"  • {node_count} nodes")
        print(f"  • {edge_count} edges")
    else:
        print("\n✗ Graph may have issues - please review the errors above")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        graph_name = sys.argv[1]
        verify_graph(graph_name)
    else:
        verify_graph()

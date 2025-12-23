# quick_check.py
"""Quick verification script for Apache AGE graphs"""

from db_connection import get_connection
from config import GRAPH_NAME

def quick_check(graph_name=GRAPH_NAME):
    """Perform a quick verification of the graph."""
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        print(f"\nQuick Graph Check: {graph_name}")
        print("-" * 50)
        
        # Node count
        cur.execute(f"""
            SELECT * FROM cypher('{graph_name}', $$
                MATCH (n) RETURN count(n) as total
            $$) as (result agtype);
        """)
        node_count = int(cur.fetchone()[0])
        print(f"Total nodes: {node_count}")
        
        # Edge count
        cur.execute(f"""
            SELECT * FROM cypher('{graph_name}', $$
                MATCH ()-[r]->() RETURN count(r) as total
            $$) as (result agtype);
        """)
        edge_count = int(cur.fetchone()[0])
        print(f"Total edges: {edge_count}")
        
        # Node types
        cur.execute(f"""
            SELECT * FROM cypher('{graph_name}', $$
                MATCH (n) 
                WITH DISTINCT LABELS(n) AS temp, COUNT(n) AS tempCnt
                UNWIND temp AS label
                RETURN label, SUM(tempCnt) AS cnt
            $$) as (label agtype, cnt agtype);
        """)
        
        print("\nNode types:")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]}")
        
        # Edge types
        cur.execute(f"""
            SELECT * FROM cypher('{graph_name}', $$
                MATCH ()-[r]->()
                WITH DISTINCT TYPE(r) AS temp, COUNT(r) AS tempCnt
                UNWIND temp AS edge_type
                RETURN edge_type, SUM(tempCnt) as cnt
            $$) as (edge_type agtype, cnt agtype);
        """)
        
        print("\nEdge types:")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]}")
        
        print("\n" + "✓" * 50)
        
        if node_count > 0 and edge_count > 0:
            print("Graph looks good!")
        else:
            print("WARNING: Graph may be empty!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        quick_check(sys.argv[1])
    else:
        quick_check()

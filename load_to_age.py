# load_to_age.py
import pandas as pd
import json
from db_connection import get_connection, create_graph, setup_age_environment
from config import GRAPH_NAME

def load_nodes_to_age(nodes_df, graph_name=GRAPH_NAME):
    """Load nodes from DataFrame into AGE graph."""
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        for _, row in nodes_df.iterrows():
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
        
        conn.commit()
        print(f"Loaded {len(nodes_df)} nodes into graph '{graph_name}'")
        
    except Exception as e:
        conn.rollback()
        print(f"Error loading nodes: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def load_edges_to_age(edges_df, graph_name=GRAPH_NAME):
    """Load edges from DataFrame into AGE graph."""
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        for _, row in edges_df.iterrows():
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
            
            cur.execute(cypher_query)
        
        conn.commit()
        print(f"Loaded {len(edges_df)} edges into graph '{graph_name}'")
        
    except Exception as e:
        conn.rollback()
        print(f"Error loading edges: {e}")
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
        
        # Get all vertex labels
        cur.execute(f"""
            SELECT label FROM ag_catalog.ag_label 
            WHERE graph = (SELECT graphid FROM ag_catalog.ag_graph WHERE name = '{graph_name}')
            AND kind = 'v';
        """)
        
        labels = [row[0] for row in cur.fetchall()]
        
        for label in labels:
            try:
                # Create index on id property
                index_query = f"""
                CREATE INDEX IF NOT EXISTS {label}_id_idx 
                ON {graph_name}."{label}" ((properties->'id'));
                """
                cur.execute(index_query)
                print(f"Created index for {label}.id")
            except Exception as e:
                print(f"Note: Index might already exist for {label}: {e}")
        
        conn.commit()
        print("Indexes created successfully")
        
    except Exception as e:
        conn.rollback()
        print(f"Error creating indexes: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    # Setup
    setup_age_environment()
    create_graph(GRAPH_NAME)
    
    # Load data
    nodes_df = pd.read_csv('nodes.csv')
    edges_df = pd.read_csv('edges.csv')
    
    load_nodes_to_age(nodes_df)
    load_edges_to_age(edges_df)
    create_indexes()
    
    print("\nData loading complete!")

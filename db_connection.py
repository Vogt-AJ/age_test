# db_connection.py
import psycopg2
from config import DB_CONFIG, GRAPH_NAME

def get_connection():
    """Create and return a database connection."""
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

def setup_age_environment():
    """Load AGE extension and set search path."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        conn.commit()
        print("AGE environment configured successfully")
    except Exception as e:
        print(f"Error setting up AGE: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def create_graph(graph_name=GRAPH_NAME):
    """Create a graph in AGE."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        # Check if graph exists
        cur.execute(f"SELECT * FROM ag_catalog.ag_graph WHERE name = '{graph_name}';")
        if cur.fetchone():
            print(f"Graph '{graph_name}' already exists")
        else:
            cur.execute(f"SELECT create_graph('{graph_name}');")
            conn.commit()
            print(f"Graph '{graph_name}' created successfully")
    except Exception as e:
        conn.rollback()
        print(f"Error creating graph: {e}")
        raise
    finally:
        cur.close()
        conn.close()

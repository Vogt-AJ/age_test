# query_graph.py
from db_connection import get_connection
from config import GRAPH_NAME

def run_cypher_query(query, graph_name=GRAPH_NAME):
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

# Example queries
if __name__ == "__main__":
    print("="*60)
    print("Graph Query Examples")
    print("="*60)
    
    # Count nodes by type
    print("\n1. Node counts by type:")
    result = run_cypher_query("MATCH (n) RETURN labels(n)[0] as label, count(n) as count")
    for row in result:
        print(f"   {row[0]}")
    
    # Find people who work at companies
    print("\n2. People working at companies (first 10):")
    result = run_cypher_query("""
        MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
        RETURN p.name, r.position, c.name
        LIMIT 10
    """)
    for row in result:
        print(f"   {row[0]}")
    
    # Find people who purchased products
    print("\n3. Recent purchases (first 10):")
    result = run_cypher_query("""
        MATCH (p:Person)-[r:PURCHASED]->(prod:Product)
        RETURN p.name, prod.name, r.quantity, r.purchase_date
        LIMIT 10
    """)
    for row in result:
        print(f"   {row[0]}")
    
    # Find companies in locations
    print("\n4. Companies and their locations:")
    result = run_cypher_query("""
        MATCH (c:Company)-[r:LOCATED_IN]->(l:Location)
        RETURN c.name, l.name, l.population
        LIMIT 10
    """)
    for row in result:
        print(f"   {row[0]}")
    
    # Find social networks (people who know each other)
    print("\n5. Social connections (first 10):")
    result = run_cypher_query("""
        MATCH (p1:Person)-[r:KNOWS]->(p2:Person)
        RETURN p1.name, r.relationship, p2.name
        LIMIT 10
    """)
    for row in result:
        print(f"   {row[0]}")
    
    # Find people who work at the same company
    print("\n6. Colleagues (people at same company):")
    result = run_cypher_query("""
        MATCH (p1:Person)-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(p2:Person)
        WHERE p1.name < p2.name
        RETURN p1.name, p2.name, c.name
        LIMIT 10
    """)
    for row in result:
        print(f"   {row[0]}")
    
    # Find people by age range
    print("\n7. People aged 30-40:")
    result = run_cypher_query("""
        MATCH (p:Person)
        WHERE p.age >= 30 AND p.age <= 40
        RETURN p.name, p.age
        LIMIT 10
    """)
    for row in result:
        print(f"   {row[0]}")
    
    # Find most popular products
    print("\n8. Most purchased products:")
    result = run_cypher_query("""
        MATCH (p:Person)-[r:PURCHASED]->(prod:Product)
        RETURN prod.name, prod.category, count(r) as purchase_count
        ORDER BY purchase_count DESC
        LIMIT 10
    """)
    for row in result:
        print(f"   {row[0]}")
    
    print("\n" + "="*60)

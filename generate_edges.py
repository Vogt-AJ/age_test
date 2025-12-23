# generate_edges.py
import pandas as pd
import numpy as np
import random

def generate_edges(nodes_df, edge_types, density):
    """
    Generate edges between nodes based on density.
    
    Args:
        nodes_df: DataFrame with node information
        edge_types: dict of {edge_label: (from_label, to_label, property_generator)}
        density: float between 0 and 1, representing edge density
    
    Returns:
        DataFrame with columns: edge_id, edge_label, from_id, to_id, properties
    """
    edges = []
    edge_id = 1
    
    for edge_label, (from_label, to_label, prop_generator) in edge_types.items():
        # Get nodes of specified types
        from_nodes = nodes_df[nodes_df['label'] == from_label]['id'].tolist()
        to_nodes = nodes_df[nodes_df['label'] == to_label]['id'].tolist()
        
        if not from_nodes or not to_nodes:
            print(f"Warning: No nodes found for edge type {edge_label}")
            continue
        
        # Calculate number of edges based on density
        max_edges = len(from_nodes) * len(to_nodes)
        num_edges = int(max_edges * density)
        
        # Generate random edges
        edge_count = 0
        attempts = 0
        max_attempts = num_edges * 10  # Prevent infinite loop
        
        created_edges = set()
        
        while edge_count < num_edges and attempts < max_attempts:
            from_id = random.choice(from_nodes)
            to_id = random.choice(to_nodes)
            
            # Avoid duplicate edges (optional - remove if duplicates are allowed)
            edge_key = (from_id, to_id, edge_label)
            if edge_key in created_edges:
                attempts += 1
                continue
            
            created_edges.add(edge_key)
            
            properties = prop_generator() if prop_generator else {}
            
            edges.append({
                'edge_id': edge_id,
                'edge_label': edge_label,
                'from_id': from_id,
                'to_id': to_id,
                'properties': properties
            })
            
            edge_id += 1
            edge_count += 1
            attempts += 1
    
    return pd.DataFrame(edges)

# Property generators for different edge types
def works_at_properties():
    """Generate properties for WORKS_AT edges."""
    positions = ['Engineer', 'Manager', 'Director', 'Analyst', 'Consultant', 
                 'Designer', 'Developer', 'Architect', 'Specialist']
    return {
        'position': random.choice(positions),
        'since_year': random.randint(2010, 2024),
        'salary': random.randint(50000, 200000)
    }

def purchased_properties():
    """Generate properties for PURCHASED edges."""
    return {
        'quantity': random.randint(1, 10),
        'purchase_date': f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        'discount': round(random.uniform(0, 0.5), 2)
    }

def knows_properties():
    """Generate properties for KNOWS edges."""
    return {
        'since': random.randint(2000, 2024),
        'relationship': random.choice(['friend', 'colleague', 'acquaintance', 'family'])
    }

def located_in_properties():
    """Generate properties for LOCATED_IN edges."""
    return {
        'since': random.randint(2000, 2024)
    }

# Example usage
if __name__ == "__main__":
    # Load nodes
    nodes_df = pd.read_csv('nodes.csv')
    
    # Define edge types: edge_label: (from_label, to_label, property_generator)
    edge_types = {
        'WORKS_AT': ('Person', 'Company', works_at_properties),
        'PURCHASED': ('Person', 'Product', purchased_properties),
        'KNOWS': ('Person', 'Person', knows_properties),
        'LOCATED_IN': ('Company', 'Location', located_in_properties),
    }
    
    # Set density (0.1 = 10% of possible edges)
    density = 0.05
    
    df_edges = generate_edges(nodes_df, edge_types, density)
    
    # Save to CSV
    df_edges.to_csv('edges.csv', index=False)
    print(f"Generated {len(df_edges)} edges")
    print(df_edges.head(10))

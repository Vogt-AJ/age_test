# generate_nodes.py
import pandas as pd
import random
from datetime import datetime, timedelta

def generate_nodes(node_types, num_nodes_per_type):
    """
    Generate nodes with different types and properties.
    
    Args:
        node_types: dict of {label: property_generator_function}
        num_nodes_per_type: dict of {label: count}
    
    Returns:
        DataFrame with columns: id, label, properties (as dict)
    """
    nodes = []
    node_id = 1
    
    for label, count in num_nodes_per_type.items():
        if label not in node_types:
            raise ValueError(f"No property generator for label: {label}")
        
        prop_generator = node_types[label]
        
        for i in range(count):
            properties = prop_generator(i)
            nodes.append({
                'id': node_id,
                'label': label,
                'properties': properties
            })
            node_id += 1
    
    return pd.DataFrame(nodes)

# Property generators for different node types
def person_properties(index):
    """Generate properties for Person nodes."""
    first_names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry', 
                   'Iris', 'Jack', 'Kelly', 'Leo', 'Mia', 'Noah', 'Olivia', 'Paul']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 
                  'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez']
    
    return {
        'name': f"{random.choice(first_names)} {random.choice(last_names)}",
        'age': random.randint(18, 80),
        'email': f"user{index}@example.com",
        'created_at': (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat()
    }

def company_properties(index):
    """Generate properties for Company nodes."""
    industries = ['Technology', 'Finance', 'Healthcare', 'Retail', 'Manufacturing', 
                  'Education', 'Entertainment', 'Energy']
    
    return {
        'name': f"Company_{index}",
        'industry': random.choice(industries),
        'employees': random.randint(10, 10000),
        'revenue': round(random.uniform(1000000, 100000000), 2),
        'founded_year': random.randint(1980, 2023)
    }

def product_properties(index):
    """Generate properties for Product nodes."""
    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Tools', 
                  'Sports', 'Beauty', 'Home']
    
    return {
        'name': f"Product_{index}",
        'category': random.choice(categories),
        'price': round(random.uniform(10, 1000), 2),
        'in_stock': random.choice([True, False]),
        'rating': round(random.uniform(1, 5), 1)
    }

def location_properties(index):
    """Generate properties for Location nodes."""
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 
              'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']
    
    return {
        'name': random.choice(cities),
        'country': 'USA',
        'latitude': round(random.uniform(25, 50), 6),
        'longitude': round(random.uniform(-125, -65), 6),
        'population': random.randint(100000, 10000000)
    }

# Example usage
if __name__ == "__main__":
    node_types = {
        'Person': person_properties,
        'Company': company_properties,
        'Product': product_properties,
        'Location': location_properties
    }
    
    num_nodes = {
        'Person': 100,
        'Company': 20,
        'Product': 50,
        'Location': 10
    }
    
    df_nodes = generate_nodes(node_types, num_nodes)
    
    # Save to CSV
    df_nodes.to_csv('nodes.csv', index=False)
    print(f"Generated {len(df_nodes)} nodes")
    print(df_nodes.head(10))

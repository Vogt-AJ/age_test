# Docker Quick Start Guide

Complete setup guide for Apache AGE with Docker on Windows.

## Prerequisites

- Docker Desktop installed on Windows
- Python 3.x installed
- PowerShell or Command Prompt

## Step 1: Start Docker Container (Already Done!)

```bash
docker run --name age-container -p 5432:5432 -e POSTGRES_PASSWORD=mypassword -d apache/age
```

## Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

## Step 3: Test Connection

```bash
python -c "from db_connection import get_connection; conn = get_connection(); cur = conn.cursor(); cur.execute('SELECT version()'); print('✓ Connected!'); print(cur.fetchone()[0]); cur.close(); conn.close()"
```

If you see version info, you're connected!

## Step 4: Generate Test Data

```bash
# Small test dataset (100 nodes, ~50 edges)
python -c "from generate_nodes import *; from generate_edges import *; nodes_df = generate_nodes({'Person': person_properties, 'Company': company_properties}, {'Person': 100, 'Company': 20}); nodes_df.to_csv('nodes.csv', index=False); edges_df = generate_edges(nodes_df, {'WORKS_AT': ('Person', 'Company', works_at_properties)}, 0.1); edges_df.to_csv('edges.csv', index=False); print(f'Generated {len(nodes_df)} nodes, {len(edges_df)} edges')"
```

## Step 5: Load Data

```bash
python simple_loader.py --batch-size 100
```

## Step 6: Verify

```bash
python quick_check.py
```

You should see:
- Total nodes: 120
- Total edges: ~120
- Node types listed
- "Graph looks good!"

## Common Commands

### Generate Different Dataset Sizes

**Tiny (fast test):**
```bash
python -c "from generate_nodes import *; from generate_edges import *; nodes_df = generate_nodes({'Person': person_properties}, {'Person': 50}); nodes_df.to_csv('nodes.csv', index=False); edges_df = generate_edges(nodes_df, {'KNOWS': ('Person', 'Person', knows_properties)}, 0.1); edges_df.to_csv('edges.csv', index=False); print(f'{len(edges_df)} edges')"
```

**Small (1K nodes):**
```bash
python -c "from generate_nodes import *; from generate_edges import *; nodes_df = generate_nodes({'Person': person_properties, 'Company': company_properties}, {'Person': 1000, 'Company': 100}); nodes_df.to_csv('nodes.csv', index=False); edges_df = generate_edges(nodes_df, {'WORKS_AT': ('Person', 'Company', works_at_properties)}, 0.05); edges_df.to_csv('edges.csv', index=False); print(f'{len(edges_df)} edges')"
```

**Medium (10K nodes):**
```bash
python -c "from generate_nodes import *; from generate_edges import *; nodes_df = generate_nodes({'Person': person_properties, 'Company': company_properties}, {'Person': 10000, 'Company': 1000}); nodes_df.to_csv('nodes.csv', index=False); edges_df = generate_edges(nodes_df, {'WORKS_AT': ('Person', 'Company', works_at_properties)}, 0.01); edges_df.to_csv('edges.csv', index=False); print(f'{len(edges_df)} edges')"
```

### Load Data

```bash
# Load with default batch size
python simple_loader.py

# Load with custom batch size
python simple_loader.py --batch-size 500
```

### Check Your Graph

```bash
python quick_check.py
```

### Connect to PostgreSQL Directly

```bash
# From Windows
docker exec -it age-container psql -U postgres -d agdb

# In psql, run Cypher:
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

SELECT * FROM cypher('generated_graph', $$
    MATCH (n:Person)
    RETURN n.name, n.age
    LIMIT 10
$$) as (name agtype, age agtype);

\q  # to exit
```

## Docker Container Management

```bash
# Stop container
docker stop age-container

# Start container
docker start age-container

# Check if running
docker ps

# View logs
docker logs age-container

# Remove container (start fresh)
docker stop age-container
docker rm age-container
# Then run the original docker run command again
```

## Troubleshooting

### "Connection refused"
```bash
# Make sure container is running
docker ps

# If not running
docker start age-container
```

### "Password authentication failed"
Make sure `config.py` has the correct password:
```python
'password': 'mypassword',  # Must match the password from docker run command
```

### Start Completely Fresh

```bash
# Remove everything
docker stop age-container
docker rm age-container

# Start new container
docker run --name age-container -p 5432:5432 -e POSTGRES_PASSWORD=mypassword -d apache/age

# Wait a few seconds, then reconnect to psql and recreate database
docker exec -it age-container psql -U postgres
# CREATE DATABASE agdb;
# \c agdb
# CREATE EXTENSION age;
# \q
```

## File Summary

- `config.py` - Database connection settings
- `db_connection.py` - Connection utilities
- `generate_nodes.py` - Create node data
- `generate_edges.py` - Create edge data
- `load_to_age.py` - Loading functions
- `simple_loader.py` - Simple loading script
- `quick_check.py` - Verify graph
- `requirements.txt` - Python dependencies

## Next Steps

Once you've verified the test data works:

1. Generate larger datasets
2. Experiment with different batch sizes for loading
3. Query your graph using Cypher
4. Build your specific use case

You're all set! 🚀

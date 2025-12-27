# AGLoad Bulk Loading Guide

AGLoad is Apache AGE's high-performance bulk loading utility. It's **significantly faster** than Cypher-based loading for large datasets (10x-100x faster).

## Performance Comparison

| Method | Speed | Best For |
|--------|-------|----------|
| **AGLoad** | 5,000-50,000+ items/sec | Large datasets (>10,000 nodes) |
| **Cypher (batched)** | 50-500 items/sec | Small-medium datasets (<10,000 nodes) |

## Installing AGLoad

### Option 1: Build from AGE source (Recommended)

```bash
# Navigate to AGE source directory
cd ~/age/tools

# Build AGLoad
make

# Install (makes it available system-wide)
sudo make install

# Verify installation
age_load --version
```

### Option 2: Manual installation

If the above doesn't work, try:

```bash
cd ~/age/tools
gcc -o age_load age_load.c -lpq -I/usr/include/postgresql
sudo cp age_load /usr/local/bin/
```

## Using AGLoad with This Toolkit

### Quick Start (Automatic)

The toolkit automatically uses AGLoad if available, with fallback to Cypher:

```bash
# This will try AGLoad first, fallback to Cypher if needed
python main_agload.py
```

### Force Cypher Loading

If you want to use the traditional Cypher method:

```bash
python main_agload.py --use-cypher
```

### AGLoad with Custom Settings

```bash
# Large dataset with AGLoad
python main_agload.py --persons 100000 --companies 5000 --density 0.01

# Keep temporary CSV files for inspection
python main_agload.py --no-cleanup
```

## Manual AGLoad Usage

### Step 1: Generate CSV files in AGLoad format

```bash
python -c "
from agload_loader import prepare_vertex_csv, prepare_edge_csv
import pandas as pd

nodes_df = pd.read_csv('nodes.csv')
edges_df = pd.read_csv('edges.csv')

vertex_files = prepare_vertex_csv(nodes_df)
edge_files = prepare_edge_csv(edges_df)
"
```

### Step 2: Load vertices

```bash
age_load \
  --dbname agdb \
  --host localhost \
  --port 5432 \
  --username your_username \
  --password your_password \
  --graph generated_graph \
  --label Person \
  --type vertex \
  --csv-path vertices_Person.csv
```

### Step 3: Load edges

```bash
age_load \
  --dbname agdb \
  --host localhost \
  --port 5432 \
  --username your_username \
  --password your_password \
  --graph generated_graph \
  --label WORKS_AT \
  --type edge \
  --csv-path edges_WORKS_AT.csv
```

## CSV Format Requirements

### Vertex CSV Format

```csv
id,name,age,email,created_at
1,Alice Smith,25,user0@example.com,2024-01-15T10:30:00
2,Bob Johnson,32,user1@example.com,2024-02-20T14:15:00
```

**Requirements:**
- First column: vertex ID (must match for edges)
- Remaining columns: vertex properties
- Header row required
- No quotes needed for simple values

### Edge CSV Format

```csv
start_id,end_id,position,since_year,salary
1,10,Engineer,2020,85000
2,10,Manager,2018,120000
```

**Requirements:**
- `start_id`: ID of source vertex
- `end_id`: ID of target vertex
- Remaining columns: edge properties
- Header row required
- Both vertices must exist before loading edges

## Troubleshooting

### AGLoad not found

```bash
# Check if installed
which age_load

# If not found, add to PATH or use full path
/path/to/age/tools/age_load --version

# Or reinstall
cd ~/age/tools
sudo make install
```

### Permission errors

```bash
# Make sure your PostgreSQL user has proper permissions
psql -U postgres -d agdb
GRANT ALL PRIVILEGES ON DATABASE agdb TO your_username;
GRANT ALL PRIVILEGES ON SCHEMA ag_catalog TO your_username;
```

### Vertex not found errors

This happens when edges reference non-existent vertices:
- Always load **vertices before edges**
- Ensure vertex IDs in edge CSV match actual vertex IDs
- Check for typos in CSV files

### Performance issues

```bash
# For very large datasets, you might need to adjust PostgreSQL settings
# Edit postgresql.conf:
shared_buffers = 2GB
work_mem = 256MB
maintenance_work_mem = 1GB
```

## Complete Examples

### Example 1: Small graph with AGLoad

```bash
# Generate and load (auto-detects AGLoad)
python main_agload.py --persons 1000 --companies 100 --products 500

# Output:
# Loading method: AGLoad (bulk)
# Average rate: 15,234 items/sec
```

### Example 2: Large graph with AGLoad

```bash
# 100K nodes, 500K edges
python main_agload.py \
  --persons 80000 \
  --companies 5000 \
  --products 10000 \
  --locations 5000 \
  --density 0.01

# Expected: Loads in 30-60 seconds vs 30-60 minutes with Cypher
```

### Example 3: Force Cypher for comparison

```bash
# Same dataset but with Cypher (much slower)
python main_agload.py \
  --persons 1000 \
  --use-cypher \
  --batch-size 500
```

### Example 4: Standalone AGLoad script

```bash
# Just run the AGLoad loader
python agload_loader.py --graph-name my_graph --no-cleanup
```

## Python API

You can also use AGLoad programmatically:

```python
from agload_loader import load_with_agload_from_dataframes
import pandas as pd

# Load your data
nodes_df = pd.read_csv('nodes.csv')
edges_df = pd.read_csv('edges.csv')

# Bulk load with AGLoad
stats = load_with_agload_from_dataframes(
    nodes_df, 
    edges_df, 
    graph_name='my_graph',
    cleanup=True  # Remove CSV files after loading
)

print(f"Loaded {stats['vertices_loaded']} vertices in {stats['time_taken']:.2f}s")
print(f"Rate: {stats['rate']:.1f} items/sec")
```

## When to Use AGLoad vs Cypher

### Use AGLoad when:
- ✓ Loading >10,000 nodes
- ✓ Initial bulk data import
- ✓ Performance is critical
- ✓ Data is already in CSV format

### Use Cypher when:
- ✓ Small datasets (<10,000 nodes)
- ✓ Incremental updates
- ✓ AGLoad not available
- ✓ Complex data transformations needed
- ✓ Testing/development with frequent reloads

## Best Practices

1. **Always load vertices before edges**
2. **Use AGLoad for initial bulk loading**
3. **Use Cypher for incremental updates**
4. **Keep CSV files for debugging** (use `--no-cleanup`)
5. **Monitor PostgreSQL logs** for errors during loading
6. **Create indexes after loading**, not before

## References

- [AGLoad Documentation](https://age.apache.org/age-manual/master/intro/agload.html)
- [Apache AGE GitHub](https://github.com/apache/age)

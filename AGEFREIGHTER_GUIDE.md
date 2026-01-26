# CSVFreighter Bulk Loading Guide

CSVFreighter (from the agefreighter package) is a Python tool designed for bulk loading data into Apache AGE using async operations. It should be significantly faster than individual Cypher queries.

## Installation

```bash
# Install agefreighter along with other dependencies
pip install -r requirements.txt

# Or install just agefreighter
pip install agefreighter==0.9.0
```

## Usage

### Step 1: Generate Your Data

```bash
python -c "from generate_nodes import *; from generate_edges import *; nodes_df = generate_nodes({'Person': person_properties, 'Company': company_properties}, {'Person': 1000, 'Company': 100}); nodes_df.to_csv('nodes.csv', index=False); edges_df = generate_edges(nodes_df, {'WORKS_AT': ('Person', 'Company', works_at_properties)}, 0.05); edges_df.to_csv('edges.csv', index=False); print(f'Generated {len(nodes_df)} nodes, {len(edges_df)} edges')"
```

### Step 2: Load with CSVFreighter

```bash
python csvfreighter_loader.py
```

### Step 3: Drop and Reload (Optional)

```bash
# Drop existing graph and reload
python csvfreighter_loader.py --drop-graph
```

## How It Works

CSVFreighter has a specific CSV format requirement:
- Each CSV must contain: start_vertex properties, edge properties, end_vertex properties
- All in a single row representing one edge with its connected vertices

The `csvfreighter_loader.py` script automatically:
1. Reads your `nodes.csv` and `edges.csv`
2. Combines them into CSVFreighter's expected format
3. Creates temporary CSV files
4. Loads them using the async CSVFreighter API
5. Cleans up temporary files

## Expected Performance

CSVFreighter uses PostgreSQL's COPY internally for bulk operations:

| Dataset Size | Expected Time |
|--------------|---------------|
| 1K nodes, 5K edges | ~5-10 seconds |
| 10K nodes, 50K edges | ~30-60 seconds |
| 100K nodes, 500K edges | ~5-10 minutes |
| 1M nodes, 5M edges | ~30-60 minutes |

This should be **2-5x faster** than the simple_loader.py method!

## Complete Workflow

```bash
# 1. Make sure Docker container is running
docker ps

# 2. Install dependencies (first time only)
pip install -r requirements.txt

# 3. Generate data
python -c "from generate_nodes import *; from generate_edges import *; nodes_df = generate_nodes({'Person': person_properties}, {'Person': 10000}); nodes_df.to_csv('nodes.csv', index=False); edges_df = generate_edges(nodes_df, {'KNOWS': ('Person', 'Person', knows_properties)}, 0.01); edges_df.to_csv('edges.csv', index=False); print(f'{len(edges_df)} edges')"

# 4. Load it
python csvfreighter_loader.py

# 5. Verify
python quick_check.py
```

## Troubleshooting

### "Module 'agefreighter' not found"

```bash
pip install agefreighter==0.9.0
```

### "Connection refused"

Make sure Docker container is running:
```bash
docker ps
docker start age-container  # if not running
```

### "Event loop is closed" (Windows)

The script already handles this with:
```python
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

If you still see this error, make sure you're running Python 3.8+

### Loading fails with errors

Check your CSV files have the correct format:
```bash
# Check nodes.csv
head -5 nodes.csv

# Check edges.csv  
head -5 edges.csv
```

## Clear and Reload

```bash
# Clear existing graph and reload
python csvfreighter_loader.py --drop-graph

# Or manually clear:
python -c "from db_connection import get_connection; conn = get_connection(); cur = conn.cursor(); cur.execute(\"LOAD 'age';\"); cur.execute(\"SET search_path = ag_catalog, '\$user', public;\"); cur.execute(\"SELECT drop_graph('generated_graph', true);\"); conn.commit(); cur.close(); conn.close(); print('Graph cleared')"
```

## Large Dataset Example

For 1M+ edges:

```bash
# Generate 1M edges
python -c "from generate_nodes import *; from generate_edges import *; nodes_df = generate_nodes({'Person': person_properties, 'Company': company_properties}, {'Person': 100000, 'Company': 10000}); nodes_df.to_csv('nodes.csv', index=False); edges_df = generate_edges(nodes_df, {'WORKS_AT': ('Person', 'Company', works_at_properties)}, 0.01); edges_df.to_csv('edges.csv', index=False); print(f'Generated {len(nodes_df)} nodes, {len(edges_df)} edges')"

# Load (should take 30-60 minutes)
python csvfreighter_loader.py
```

## CSV Format

CSVFreighter expects CSVs with this structure:

```
start_id,start_name,start_age,edge_since,edge_position,end_id,end_name,end_industry
1,Alice,25,2020,Engineer,100,CompanyA,Tech
2,Bob,30,2019,Manager,100,CompanyA,Tech
```

The script automatically creates this format from your `nodes.csv` and `edges.csv`.

## Advantages

✓ **Fast** - Uses PostgreSQL COPY internally  
✓ **Async** - Non-blocking operations  
✓ **No compilation** - Pure Python package  
✓ **Automatic conversion** - Script handles CSV transformation  
✓ **Progress tracking** - Shows loading progress  

## When to Use

- **Use CSVFreighter for**: Most cases, especially 10K+ edges
- **Use simple_loader for**: Very small datasets, testing, debugging
- **Use AGLoad for**: 10M+ edges (if you can install it)

Try it out and see the performance difference!


## Installation

```bash
# Install agefreighter along with other dependencies
pip install -r requirements.txt

# Or install just agefreighter
pip install agefreighter==0.9.0
```

## Usage

### Step 1: Generate Your Data

```bash
python -c "from generate_nodes import *; from generate_edges import *; nodes_df = generate_nodes({'Person': person_properties, 'Company': company_properties}, {'Person': 1000, 'Company': 100}); nodes_df.to_csv('nodes.csv', index=False); edges_df = generate_edges(nodes_df, {'WORKS_AT': ('Person', 'Company', works_at_properties)}, 0.05); edges_df.to_csv('edges.csv', index=False); print(f'Generated {len(nodes_df)} nodes, {len(edges_df)} edges')"
```

### Step 2: Load with AgeFreighter

```bash
python agefreighter_loader.py
```

That's it! Much simpler than the batch loading method.

## Expected Performance

AgeFreighter uses bulk operations internally, so it should be faster:

| Dataset Size | Expected Time |
|--------------|---------------|
| 1K nodes, 5K edges | ~5-10 seconds |
| 10K nodes, 50K edges | ~30-60 seconds |
| 100K nodes, 500K edges | ~5-10 minutes |
| 1M nodes, 5M edges | ~30-60 minutes |

This should be **2-5x faster** than our simple_loader.py method!

## How It Works

AgeFreighter:
1. Connects to your PostgreSQL/AGE database
2. Reads your CSV files
3. Uses optimized bulk loading methods internally
4. Groups operations by label/type for efficiency
5. Handles property conversion automatically

## Comparison with Other Methods

| Method | Speed | Complexity | Installation |
|--------|-------|-----------|--------------|
| **AgeFreighter** | Fast | Very Easy | `pip install` |
| **AGLoad** | Fastest | Hard | Build from source |
| **Simple Loader** | Slow | Easy | No extra install |

## Complete Workflow

```bash
# 1. Make sure Docker container is running
docker ps

# 2. Install dependencies (first time only)
pip install -r requirements.txt

# 3. Generate data
python -c "from generate_nodes import *; from generate_edges import *; nodes_df = generate_nodes({'Person': person_properties}, {'Person': 10000}); nodes_df.to_csv('nodes.csv', index=False); edges_df = generate_edges(nodes_df, {'KNOWS': ('Person', 'Person', knows_properties)}, 0.01); edges_df.to_csv('edges.csv', index=False); print(f'{len(edges_df)} edges')"

# 4. Load it
python agefreighter_loader.py

# 5. Verify
python quick_check.py
```

## Troubleshooting

### "Module 'agefreighter' not found"

```bash
pip install agefreighter==0.9.0
```

### "Connection refused"

Make sure Docker container is running:
```bash
docker ps
docker start age-container  # if not running
```

### "Graph does not exist"

Create the graph first:
```bash
docker exec -it age-container psql -U postgres -d agdb -c "LOAD 'age'; SET search_path = ag_catalog, '\$user', public; SELECT create_graph('generated_graph');"
```

### Loading fails with errors

Check your CSV files have the correct format:
```bash
# Check nodes.csv
head -5 nodes.csv

# Check edges.csv  
head -5 edges.csv
```

## Clear and Reload

```bash
# Clear existing graph
python -c "from db_connection import get_connection; conn = get_connection(); cur = conn.cursor(); cur.execute(\"LOAD 'age';\"); cur.execute(\"SET search_path = ag_catalog, '\$user', public;\"); cur.execute(\"SELECT drop_graph('generated_graph', true);\"); cur.execute(\"SELECT create_graph('generated_graph');\"); conn.commit(); cur.close(); conn.close(); print('Graph cleared')"

# Generate new data
python -c "from generate_nodes import *; from generate_edges import *; nodes_df = generate_nodes({'Person': person_properties}, {'Person': 5000}); nodes_df.to_csv('nodes.csv', index=False); edges_df = generate_edges(nodes_df, {'KNOWS': ('Person', 'Person', knows_properties)}, 0.02); edges_df.to_csv('edges.csv', index=False); print(f'{len(edges_df)} edges')"

# Load with agefreighter
python agefreighter_loader.py
```

## Large Dataset Example

For 1M+ edges:

```bash
# Generate 1M edges
python -c "from generate_nodes import *; from generate_edges import *; nodes_df = generate_nodes({'Person': person_properties, 'Company': company_properties}, {'Person': 100000, 'Company': 10000}); nodes_df.to_csv('nodes.csv', index=False); edges_df = generate_edges(nodes_df, {'WORKS_AT': ('Person', 'Company', works_at_properties)}, 0.01); edges_df.to_csv('edges.csv', index=False); print(f'Generated {len(nodes_df)} nodes, {len(edges_df)} edges')"

# Load (should take 30-60 minutes)
python agefreighter_loader.py
```

## Advantages

✓ **Fast** - Uses bulk operations  
✓ **Simple** - Just one Python script  
✓ **No compilation** - Pure Python package  
✓ **Automatic conversions** - Handles property types  
✓ **Progress tracking** - Shows what's happening  

## When to Use

- **Use AgeFreighter for**: Most cases, especially 10K+ edges
- **Use simple_loader for**: Very small datasets, testing, debugging
- **Use AGLoad for**: 10M+ edges (if you can install it)

Try it out and see how much faster it is!
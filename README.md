# Apache AGE Graph Generator

A Python toolkit for generating and loading graph data into Apache AGE with PostgreSQL 17.

## Prerequisites

- Ubuntu 24.04
- PostgreSQL 17 with Apache AGE 1.6.0 installed
- Python 3.x

## Installation

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure database connection:**
   Edit `config.py` and update your database credentials:
   ```python
   DB_CONFIG = {
       'host': 'localhost',
       'database': 'agdb',
       'user': 'your_username',      # Your PostgreSQL username
       'password': 'your_password',  # Your PostgreSQL password
       'port': 5432
   }
   ```

## Usage

### Quick Start

Generate a graph with default settings (100 persons, 20 companies, 50 products, 10 locations, 5% density):

```bash
python main.py
```

### Custom Configuration

Generate a custom graph:

```bash
python main.py --persons 500 --companies 50 --products 200 --locations 25 --density 0.1 --graph-name my_graph
```

**Parameters:**
- `--persons`: Number of Person nodes (default: 100)
- `--companies`: Number of Company nodes (default: 20)
- `--products`: Number of Product nodes (default: 50)
- `--locations`: Number of Location nodes (default: 10)
- `--density`: Edge density from 0.0 to 1.0 (default: 0.05 = 5%)
- `--graph-name`: Name of the graph in AGE (default: 'generated_graph')

### Step-by-Step Usage

You can also run each step individually:

1. **Generate nodes:**
   ```bash
   python generate_nodes.py
   ```
   Creates `nodes.csv` with node data.

2. **Generate edges:**
   ```bash
   python generate_edges.py
   ```
   Creates `edges.csv` with edge data (requires `nodes.csv`).

3. **Load to AGE:**
   ```bash
   python load_to_age.py
   ```
   Loads data from CSV files into Apache AGE and creates indexes.

### Query the Graph

Run example queries:

```bash
python query_graph.py
```

Or write custom queries in your own scripts:

```python
from query_graph import run_cypher_query

# Simple query
result = run_cypher_query("MATCH (n:Person) RETURN n.name LIMIT 10")
for row in result:
    print(row)
```

## Graph Schema

### Node Types

1. **Person**
   - Properties: id, name, age, email, created_at

2. **Company**
   - Properties: id, name, industry, employees, revenue, founded_year

3. **Product**
   - Properties: id, name, category, price, in_stock, rating

4. **Location**
   - Properties: id, name, country, latitude, longitude, population

### Edge Types

1. **WORKS_AT** (Person → Company)
   - Properties: position, since_year, salary

2. **PURCHASED** (Person → Product)
   - Properties: quantity, purchase_date, discount

3. **KNOWS** (Person → Person)
   - Properties: since, relationship

4. **LOCATED_IN** (Company → Location)
   - Properties: since

## Files

- `config.py` - Database configuration
- `db_connection.py` - Database connection utilities
- `generate_nodes.py` - Node generation logic
- `generate_edges.py` - Edge generation logic
- `load_to_age.py` - Load data into AGE
- `main.py` - Main orchestration script
- `query_graph.py` - Example queries
- `requirements.txt` - Python dependencies

## Output Files

- `nodes.csv` - Generated node data
- `edges.csv` - Generated edge data

## Customization

### Adding New Node Types

Edit `generate_nodes.py` and add a property generator function:

```python
def my_node_properties(index):
    return {
        'property1': 'value',
        'property2': 123
    }
```

Then update the `node_types` dict in `main.py`.

### Adding New Edge Types

Edit `generate_edges.py` and add a property generator function:

```python
def my_edge_properties():
    return {
        'property1': 'value'
    }
```

Then update the `edge_types` dict in `main.py`.

## Troubleshooting

**Connection errors:**
- Verify PostgreSQL is running: `sudo systemctl status postgresql`
- Check credentials in `config.py`
- Ensure AGE extension is loaded: `LOAD 'age';`

**Permission errors:**
- Make sure your user has proper permissions on the database
- See the setup guide for granting permissions

**Import errors:**
- Install dependencies: `pip install -r requirements.txt`

## Examples

```bash
# Small test graph
python main.py --persons 50 --companies 10 --products 20 --density 0.1

# Large graph
python main.py --persons 10000 --companies 500 --products 2000 --locations 100 --density 0.01

# Sparse social network
python main.py --persons 1000 --companies 100 --products 500 --density 0.005
```

## License

MIT License

# config.py
import os

DB_CONFIG = {
    'host': 'localhost',      # Docker maps to localhost
    'database': 'agdb',
    'user': 'postgres',       # Default user in Docker
    'password': 'mypassword', # Password you set when running docker
    'port': 5432,
    'sslmode': 'disable'
}

GRAPH_NAME = 'generated_graph'

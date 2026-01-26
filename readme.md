# Pull the Apache AGE Docker image
docker pull apache/age

# Run the container
docker run --name age-container -p 5432:5432 -e POSTGRES_PASSWORD=mypassword -d apache/age

# Verify it's running
docker ps

# Connect
# Connect to the container
docker exec -it age-container psql -U postgres

# You should now be in the psql prompt

# INIT database
-- Create database
CREATE DATABASE agdb;

-- Connect to it
\c agdb

-- Create AGE extension
CREATE EXTENSION IF NOT EXISTS age;

-- Load AGE
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

-- Create graph
SELECT create_graph('generated_graph');

-- Verify
SELECT * FROM ag_catalog.ag_graph;

-- Exit
\q


# python venv

python -m venv /venv
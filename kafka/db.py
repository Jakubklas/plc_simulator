import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time

# Database and table setup
def setup_postgres():
    # Connect to default 'postgres' database first
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="12345",
        database="postgres"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    # Create database if not exists
    cursor.execute("SELECT 1 FROM pg_database WHERE datname='readings'")
    if not cursor.fetchone():
        cursor.execute("CREATE DATABASE readings")
        print("Database 'readings' created")
    
    cursor.close()
    conn.close()
    
    # Connect to 'readings' database and create table
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="12345",
        database="readings"
    )
    cursor = conn.cursor()
    
    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data (
            area VARCHAR(255),
            sensor_id VARCHAR(255),
            avg_value DOUBLE PRECISION,
            count INTEGER,
            timestamp BIGINT
        )
    """)
    conn.commit()
    print("Table 'sensor_data' ready")
    
    cursor.close()
    conn.close()

def query_postgres():
    # Connect to 'readings' database
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="12345",
        database="readings"
    )
    cursor = conn.cursor()
    
    # Query the latest 10 records from the 'sensor_data' table
    cursor.execute("""
        SELECT COUNT(*)
        FROM sensor_data
    """)
    
    # Fetch and print the results
    rows = cursor.fetchall()
    print(f"Current # of records --> {rows[0][0]}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    while True:
        query_postgres()
        print("-"*10,f"10 latest rows as of {time.time()}", "-"*10)
        time.sleep(1)
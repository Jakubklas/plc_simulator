from config import *
from postgres import PostgresHandle
import threading
import time
import tabulate
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('database.log')
    ]
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # Connect to the DB & create table
    pg = PostgresHandle()
    pg.create_tables()

    # Generate data in a thread
    thread = threading.Thread(target=pg.generator)
    thread.daemon=True
    thread.start()

    # Query the DB perpetually
    try:
        while True:
            time.sleep(7)
            rows, cols = pg.query(
                """
                SELECT 
                    r.sensor
                    , o.location
                    , l.site_name
                    , l.address_number
                    , r.value
                    , r.unit
                    , o.up_time
                    
                FROM readings r
                LEFT JOIN oee o ON r.sensor=o.sensor_id
                LEFT JOIN locations l ON o.location = l.location_code

                ORDER BY o.up_time DESC
                LIMIT 6;
                """
            )
            print("\n",tabulate.tabulate(rows, cols, "grid"), "\n")
    except KeyboardInterrupt as e:
            logger.warning(f"Stopped reading data:\n{e}")

    
         

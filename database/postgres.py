import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.pool import SimpleConnectionPool
import logging
import time
import random
from config import *

logger = logging.getLogger(__name__)

class PostgresHandle:
    def __init__(self) -> None:
        self.pool = SimpleConnectionPool(
            minconn=5,
            maxconn=10,
            database=dbconfig["database"],
            user=dbconfig["db_user"],
            password=dbconfig["password"],
            host=dbconfig["host"],
            port=dbconfig["port"]
        )

    def get_conn(self):
        try:
            conn = self.pool.getconn()
            logger.info("Connection received")
            return conn
        except Exception as e:
            logger.error(f"Error connecting to DB:\n{e}")
            raise
    
    def put_conn(self, conn):
        try:
            self.pool.putconn(conn)
            logger.info("Connection returned")
            return conn
        except Exception as e:
            logger.error(f"Error returning connection to the pool:\n{e}")
            raise
    
    def create_table(self, drop=True) -> None:
        try:
            conn = self.get_conn()
            with conn.cursor() as cursor:
                if drop:
                    cursor.execute(
                        """
                        DROP TABLE IF EXISTS readings;
                        """
                    )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS readings (
                        sensor VARCHAR(50),
                        unit VARCHAR(50),
                        value DECIMAL(5, 2),
                        record_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (sensor, record_time)
                    );
                    """
                )
            logger.info("Table created successfuly.")
            conn.commit()
            logger.info("Changes commited.")
        except Exception as e:
            logger.error(f"Error returning connection to the pool:\n{e}")
            conn.rollback()
            raise
        finally:
            self.put_conn(conn)

    def insert_data(self, data: list[tuple]) -> None:
        try:
            conn = self.get_conn()
            sql="""
                INSERT INTO readings (sensor, unit, value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (sensor, record_time)
                    DO UPDATE SET
                        value = EXCLUDED.value
                        , unit = EXCLUDED.unit
                    ;
                """
            with conn.cursor() as cursor:
                execute_batch(cur=cursor, sql=sql, argslist=data, page_size=100)
                logger.info(f"Inserted {len(data)} rows into the database")
            
            conn.commit()
            logger.info("Changes commited.")
        except Exception as e:
            logger.error(f"Error returning connection to the pool:\n{e}")
            conn.rollback()
            raise
        finally:
            self.put_conn(conn)

    def query(self, sql):
        try:
            conn = self.get_conn()
            with conn.cursor() as cursor:
                logger.info("\nQuerying DB...")
                cursor.execute(sql)
                result = cursor.fetchall()
                logger.info(f"Got {len(result)} results...")

                return (
                    result,
                    [desc[0] for desc in cursor.description]
                )
            
        except Exception as e:
            logger.error(f"Error querying database: {e}")
            raise
        finally:
            self.put_conn(conn)

    def generate_row(self):
        try:
            sensor = random.choice(["theremometer", "flowmeter", "barometer"])
            unit = {
                "theremometer": "C",
                "flowmeter": "m3/s",
                "barometer": "psi"
            }[sensor]
            sensor = f"{sensor}_{int(random.uniform(1, 4))}"
            value = random.uniform(0.12, 56.87)
            return (sensor, unit, value)
        
        except Exception as e:
            logger.error(f"Stopped generating data:\n{e}")
    
    def generator(self):
        try:
            while True:
                rows = []
                while len(rows) < 100:
                    rows.append(self.generate_row())
                    if len(rows) % 20 == 0:
                        logger.info(f"Generated {len(rows)} rows...")
                    time.sleep(0.1)               
                self.insert_data(rows)
                
        except KeyboardInterrupt as e:
                logger.warning(f"Stopped generating data:\n{e}")
        finally:
            if len(rows) > 0:
                self.insert_data(rows)
                rows=[]
        
            


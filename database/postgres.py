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
    
    def create_tables(self, drop=True) -> None:
        try:
            conn = self.get_conn()
            with conn.cursor() as cursor:
                if drop:
                    cursor.execute(
                        """
                        DROP TABLE IF EXISTS readings;
                        DROP TABLE IF EXISTS oee;
                        -- DROP TABLE IF EXISTS locations;
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
                logger.info("'readings' table created successfuly.")
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS oee (
                        sensor_id VARCHAR(50),
                        up_time DECIMAL(5, 2),
                        location VARCHAR(100),
                        record_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (sensor_id, record_time)
                    );
                    """
                )
                logger.info("'oee' table created successfuly.")
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS locations (
                        location_code VARCHAR(100) PRIMARY KEY,
                        site_name VARCHAR(100),
                        address_number INT
                    );
                    """
                )
                logger.info("'locations' table created successfuly.")

            conn.commit()
            logger.info("Changes commited.")
        except Exception as e:
            logger.error(f"Error returning connection to the pool:\n{e}")
            conn.rollback()
            raise
        finally:
            self.put_conn(conn)

    def insert_readings(self, data: list[tuple]) -> None:
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

    def insert_oee(self, data: list[tuple]) -> None:
        try:
            conn = self.get_conn()
            sql="""
                INSERT INTO oee (sensor_id, up_time, location)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (sensor_id, record_time)
                    DO UPDATE SET
                        up_time = EXCLUDED.up_time
                        , location = EXCLUDED.location
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

    def insert_locations(self, data: list[tuple]) -> None:
        try:
            conn = self.get_conn()
            sql="""
                INSERT INTO locations (location_code, site_name, address_number)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (location_code)
                    DO NOTHING;
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

    def generate_rows(self, table):
        if table == "readings":
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

        elif table == "oee":
            try:
                sensor_id = random.choice(["theremometer", "flowmeter", "barometer"])
                sensor_id = f"{sensor_id}_{int(random.uniform(1, 4))}"
                up_time = random.uniform(0.23, 100.0)
                location = random.choice(["LBA3", "HBB1", "YYK2"])
                return (sensor_id, up_time, location)
            
            except Exception as e:
                logger.error(f"Stopped generating data:\n{e}")
        
        elif table == "locations":
            try:
                location_code = random.choice(["LBA3", "HBB1", "YYK2"])
                site_name = {
                    "LBA3": "London",
                    "HBB1": "Manchester",
                    "YYK2": "Yorkshire"
                }[location_code]
                address_number = random.randint(1, 50)
                return (location_code, site_name, address_number)
            
            except Exception as e:
                logger.error(f"Stopped generating data:\n{e}")

        else:
            raise ValueError(f"Incorrect table name. Tabel '{table}' does not exist.")
    
    def generator(self):
        try:
            while True:
                readings = []
                oee = []
                locations = []
                while len(readings) < 100:
                    readings.append(self.generate_rows("readings"))
                    oee.append(self.generate_rows("oee"))
                    if len(readings) % 20 == 0:
                        logger.info(f"Generated {len(readings)} rows...")
                        locations.append(self.generate_rows("locations"))
                    time.sleep(0.1)               
                self.insert_readings(readings)
                self.insert_oee(oee)
                self.insert_locations(locations)
                
        except KeyboardInterrupt as e:
                logger.warning(f"Stopped generating data:\n{e}")
        finally:
            if len(readings) > 0:
                self.insert_readings(readings)
                readings=[]
        
            


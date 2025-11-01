import dotenv
import os
import logging

logger = logging.getLogger(__name__)

dotenv.load_dotenv()

dbconfig = {
    "database": os.getenv("database"),
    "db_user": os.getenv("db_user"),
    "password": os.getenv("password"),
    "port": os.getenv("port"),
    "host": os.getenv("host")
}
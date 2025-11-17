import logging
import asyncio
from threading import Thread

from config import *
from plc_1 import DesinfectionPLC

# Configure the logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Suppress asyncua address_space warnings
logging.getLogger("asyncua.server.address_space").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def example_function():
    logger.info("This is an info message from example_function.")

if __name__ == "__main__":
    try:
        logger.info(f"Starting plc 1; Params: \n\tName: {NAME}\n\tEndpoint: {ENDPOINT}\n\tURI: {URI}")
        plc_1 = DesinfectionPLC(ENDPOINT, URI, NAME)
        # thread = Thread(target=lambda: asyncio.run(plc_1.run()), daemon=True)
        asyncio.run(plc_1.run())

    except Exception as e:
        logger.error(f"Failed due to : {e}")
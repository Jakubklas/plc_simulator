import logging

from config import *
from plc_1 import DesinfectionPLC

# Configure the logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def example_function():
    logger.info("This is an info message from example_function.")

if __name__ == "__main__":
    try:
        logger.info(f"Starting plc 1; Params: \n\tName: {NAME}\n\tEndpoint: {ENDPOINT}\n\tURI: {URI}")
        plc_1 = DesinfectionPLC(ENDPOINT, URI, NAME)
    except Exception as e:
        logger.error(f"Failed due to : {e}")

    
    logger.info("Main program finished.")
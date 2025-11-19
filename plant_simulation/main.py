import logging
import asyncio
from threading import Thread

from config import *
from opcua_plc.plc_1 import PreTreatmentPLC
from opcua_plc.plc_2 import DesinfectionPLC

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

async def run_both_plcs():
    """Run both PLCs simultaneously"""
    try:
        logger.info(f"Starting both PLCs...")
        logger.info(f"PLC 1 - {NAME_PT}: {ENDPOINT_PT}")
        logger.info(f"PLC 2 - {NAME_MT}: {ENDPOINT_MT}")
        
        # Create both PLC instances
        plc_1 = PreTreatmentPLC(ENDPOINT_PT, URI_PT, NAME_PT, emit_logs=True)
        plc_2 = DesinfectionPLC(ENDPOINT_MT, URI_MT, NAME_MT, emit_logs=True)
        
        # Run both PLCs concurrently
        await asyncio.gather(
            plc_1.run(),
            plc_2.run()
        )
        
    except Exception as e:
        logger.error(f"Failed due to : {e}")

if __name__ == "__main__":
    asyncio.run(run_both_plcs())
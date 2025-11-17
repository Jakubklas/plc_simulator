from asyncua import Server
import asyncio
import time
import logging

from config import *

logger = logging.getLogger(__name__)

class DesinfectionPLC:
    def __init__(self, endpoint=ENDPOINT, uri=URI, name=NAME) -> None:
        self.server = Server()
        self.endpoint = endpoint
        self.uri = uri
        self.name = name

        # Initializeing sever values & contols

    async def start_server(self):
        # Start server at endpoint & give it name and uri
        # Get the objects node of the server & add the plant as object
        # Assign sensors/controls/setpoints/statuses as objects & variables
        # Launch the sim loop
        return
    
    async def _sim_loop(self):
        # Create  while loop
        # Assign values to all the sensors/statuses continuously if constrols allow
        # Implement a graceful server shutdown if interrupted
        # Log progress each N seconds
        return
    

if __name__ == "__main__":
    logger.info("Testing .")
    
    logger.info("Main program finished.")
from asyncua import Client
import logging
import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import *

logger = logging.getLogger(__name__)

class OPCUAMonitor:
    def __init__(self) -> None:
        self.inputs = []
        self.clients = {}

    def add_client(self, endpoint, uri, name):
        self.inputs.append(
            {
                "endpoint": endpoint,
                "uri": uri,
                "name": name
            }
        )

    async def connect(self, all=True, idx=0):
        if all:
            for input in self.inputs:
                try:
                    logger.info(f"Connecting to opc-ua server {input['name']}...")
                    client = Client(input["endpoint"])
                    await client.connect()
                    idx = await client.get_namespace_index(input["uri"])

                    self.clients[input["name"]] = {
                        "client": client,
                        "idx": idx
                    }
                    logger.info(f"Connected to {input['name']} successfully.")

                except Exception as e:
                    logger.error(f"Could not connect to {input['name']}. Error: {e}")
                    
                    
    async def read(self):
        print("""
            options = show all options
            devices = list all available devices
            read data = read data from a selected device
            """)
        while True:
            try:
                action = input("Action: ")
                if action == "options":
                    print("""
                        options = show all options
                        devices = list all available devices
                        read data = read data from a selected device
                        """)
                elif action == "devices":
                    print("Connected devices:")
                    for k in self.clients:
                        print("   --> ", k)
                elif action == "read":
                    device = input("Select device name: ")
                    if device in self.clients.keys():
                        client = self.clients[device]["client"]
                        idx = self.clients[device]["idx"]
                        
                        # Get the root objects node
                        root = client.get_objects_node()
                        
                        if device == "WaterTankPLC":
                            plant = await root.get_child([f"{idx}:WaterTank"])
                            sensors = await plant.get_child([f"{idx}:Sensors"])
                            
                            # Get individual sensor nodes and read values
                            temp_node = await sensors.get_child([f"{idx}:Water Tempereature"])
                            pressure_node = await sensors.get_child([f"{idx}:Tank Pressure"])
                            
                            print()
                            print("Water Temperature:", await temp_node.read_value())
                            print("Tank Pressure:", await pressure_node.read_value())
                            print()
                            
                        elif device == "DesinfectionPlantPLC":
                            plant = await root.get_child([f"{idx}:DesinfectionPlant"])
                            sensors = await plant.get_child([f"{idx}:Sensors"])
                            
                            # Get individual sensor nodes and read values
                            uv_node = await sensors.get_child([f"{idx}:UV Intensity"])
                            chlorine_node = await sensors.get_child([f"{idx}:Chlorine Level"])
                            turbidity_node = await sensors.get_child([f"{idx}:Turbidity"])
                            
                            print()
                            print("UV Intensity:", await uv_node.read_value())
                            print("Chlorine Level:", await chlorine_node.read_value())
                            print("Turbidity:", await turbidity_node.read_value())
                            print()
                    else: 
                        print("That device is not available\n")

            except Exception as e:
                    logger.error(f"Invalid action presented... {e}")


async def main():
    monitor = OPCUAMonitor()
    
    # Add both PLCs to monitor
    monitor.add_client(ENDPOINT_PT, URI_PT, NAME_PT)
    monitor.add_client(ENDPOINT_MT, URI_MT, NAME_MT)
    
    # Connect to all PLCs
    await monitor.connect()
    
    # Start interactive mode
    await monitor.read()

if __name__ == "__main__":
    asyncio.run(main())

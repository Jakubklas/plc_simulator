import logging
import asyncio
from threading import Thread
from opcua_server import WaterPlantPLC
import time
from config import *

# Configure basic logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

async def test_client():
    print()
    print()

    from asyncua import Client
    
    async def _conn(client, action):
        if action == "connect":
            return await client.connect()
        elif action == "disconnect":
            return await client.disconnect()

    client = Client(ENDPOINT)
    await _conn(client, "connect")

    # Root Node Information
    root = client.get_root_node()
    print("="*10,"ROOT NODE", "="*10)
    print(f"Root Node ID: {root.nodeid}")
    print(f"Root Name: {await root.read_browse_name()}\n")

    # Immediate Children of the root node
    print("="*10,"Root Node Children", "="*10)
    children = await root.get_children()
    for child in children:
        name  = await child.read_browse_name()
        node_class  = await child.read_node_class()
        print(f"----> Node name: {name}")
        print(f"----> Node class: {node_class}\n")

    # Pick the objects root child and dig deeper
    print("="*10,"Objects Node", "="*10)
    objects_node = await root.get_child(f"{0}:Objects")
    children = await objects_node.get_children()
    for child in children:
        name  = await child.read_browse_name()
        node_class  = await child.read_node_class()
        print(f"--------> Node name: {name}")
        print(f"--------> Node class: {node_class}\n")
    
    print("="*10,"WaterPlant Node", "="*10)
    waterplant_node = await objects_node.get_child(f"{2}:WaterPlant")
    children = await waterplant_node.get_children()
    for child in children:
        name  = await child.read_browse_name()
        node_class  = await child.read_node_class()
        node_value = None
        
        print(f"------------> Node name: {name}")
        print(f"------------> Node class: {node_class}")
        if node_class == 2:
            node_value = await child.read_value()
            val_type = await child.read_data_type_as_variant_type()
            print(f"------------> Node Value: {round(node_value, 2)}")
            print(f"------------> Value D-Type: {val_type}")
        print()



    await _conn(client, "disconnect")



if __name__ == "__main__":
    # Run the PLC simulation on a separate thread
    plc = WaterPlantPLC(ENDPOINT, URI, NAME)
    thread = Thread(target=lambda: asyncio.run(plc.run()), daemon=False)
    thread.start()
    time.sleep(1)

    

    # asyncio.run(test_client())
    

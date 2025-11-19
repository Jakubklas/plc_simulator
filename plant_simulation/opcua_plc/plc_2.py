from asyncua import Server
import asyncio
import time
import random
import logging

from config import *

logger = logging.getLogger(__name__)

class DesinfectionPLC:
    def __init__(self, endpoint=ENDPOINT_MT, uri=URI_MT, name=NAME_MT, emit_logs=True) -> None:
        self.server = Server()
        self.endpoint = endpoint
        self.uri = uri
        self.name = name
        self.nodes = {}
        self.emit_logs=emit_logs

        # Init sensors
        self.uv_intensity = 0.0
        self.chlorine_lvl = 0.0
        self.turbidity = 0.0

        # Init controls
        self.uv_lamp_on = False
        self.dosing_pump_on = False


    async def start_server(self):
        logger.info(f"Starting PLC server...")

        # Start server at endpoint & give it name and uri
        await self.server.init()
        self.server.set_endpoint(ENDPOINT_MT)
        self.server.set_server_name(NAME_MT)
        idx = await self.server.register_namespace(URI_MT)

        # Get the objects node of the server & add the plant as object
        objects = self.server.get_objects_node()
        plant = await objects.add_object(idx, "DesinfectionPlant")

        # Assign sensors/controls/setpoints/statuses as objects & variables
        # Sensors
        sensors = await plant.add_object(idx, "Sensors")
        self.nodes["uv"] = await sensors.add_variable(idx, "UV Intensity", 0.0)
        self.nodes["chlorine"] = await sensors.add_variable(idx, "Chlorine Level", 0.0)
        self.nodes["turbidity"] = await sensors.add_variable(idx, "Turbidity", 0.0)

        for k in ["uv", "chlorine", "turbidity"]:
            await self.nodes[k].set_writable(False)

        # Controls
        controls = await plant.add_object(idx, "Controls")
        self.nodes["uv_lamp"] = await controls.add_variable(idx, "UV Lamp On", False)
        self.nodes["dosing_pump"] = await controls.add_variable(idx, "Dosing Pump On", False)

        for k in ["uv_lamp", "dosing_pump"]:
            await self.nodes[k].set_writable(True)
    
    async def sim_loop(self):
        logger.info(f"Starting Desinfection PLC simulation...")

        # Create  while loop with simulated values
        counter = 0
        while True:
            # Simulation
            try:
                counter += 1
                self.uv_lamp_on = await self.nodes["uv_lamp"].read_value()
                self.dosing_pump_on = await self.nodes["dosing_pump"].read_value()
                
                if self.uv_lamp_on:
                    self.uv_intensity = min(80.0, self.uv_intensity + 5.0)
                else:
                    self.uv_intensity = max(0.0, self.uv_intensity - 10.0)
                
                if self.dosing_pump_on:
                    self.chlorine_lvl = min(2.5, self.chlorine_lvl + 0.1)
                else:
                    self.chlorine_lvl = max(0.0, self.chlorine_lvl - 0.05)
                
                self.turbidity = 0.5 + random.uniform(-0.1, 0.1)

                # Assign values to all the sensors/statuses continuously if constrols allow
                await self.nodes["uv"].write_value(self.uv_intensity)
                await self.nodes["chlorine"].write_value(self.chlorine_lvl)
                await self.nodes["turbidity"].write_value(self.turbidity)

                await asyncio.sleep(1)
        
                # Log progress each N seconds
                if self.emit_logs & (counter % 5 == 0) & (counter != 0):
                    logger.info(f"""
    ========= STATUS =========
    --> UV Lamp On: {self.uv_lamp_on}
    --> Dosing Pump On: {self.dosing_pump_on}

    --> UV Intensity: {self.uv_intensity:.1f}
    --> Chlorine Level: {self.chlorine_lvl:.2f}
    --> Turbidity: {self.turbidity:.2f}
                    """)

            # Graceful server shutdown if interrupted
            except KeyboardInterrupt:
                logger.info("Server shutting down...")
            except Exception as e:
                logger.error(f"Desinfection simulation error: {e}")
    
    async def run(self):
        await self.start_server()
        async with self.server: 
            await self.sim_loop()
    

if __name__ == "__main__":
    logger.info("Testing")
    
    logger.info("Main program finished.")

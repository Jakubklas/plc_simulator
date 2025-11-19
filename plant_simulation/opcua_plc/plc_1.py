from asyncua import Server
import asyncio
import time
import random
import logging

from config import *

logger = logging.getLogger(__name__)

class PreTreatmentPLC:
    def __init__(self, endpoint=ENDPOINT_PT, uri=URI_PT, name=NAME_PT, emit_logs=False) -> None:
        self.server = Server()
        self.endpoint = endpoint
        self.uri = uri
        self.name = name
        self.nodes = {}
        self.emit_logs = emit_logs

        # Init sensors
        self.temperature = 0.0
        self.tank_pressure = 0.0

        # Init controls
        self.valve_open = False
        self.heater_on = False


    async def start_server(self):
        logger.info(f"Starting PLC server...")

        # Start server at endpoint & give it name and uri
        await self.server.init()
        self.server.set_endpoint(ENDPOINT_PT)
        self.server.set_server_name(NAME_PT)
        idx = await self.server.register_namespace(URI_PT)

        # Get the objects node of the server & add the plant as object
        objects = self.server.get_objects_node()
        plant = await objects.add_object(idx, "WaterTank")

        # Assign sensors/controls/setpoints/statuses as objects & variables
        # Sensors
        sensors = await plant.add_object(idx, "Sensors")
        self.nodes["temperature"] = await sensors.add_variable(idx, "Water Tempereature", 0.0)
        self.nodes["pressure"] = await sensors.add_variable(idx, "Tank Pressure", 0.0)

        for k in ["temperature", "pressure"]:
            await self.nodes[k].set_writable(False)

        # Controls
        controls = await plant.add_object(idx, "Controls")
        self.nodes["heater"] = await controls.add_variable(idx, "Water Heater On", False)
        self.nodes["valve"] = await controls.add_variable(idx, "Valve Open", False)

        for k in ["heater", "valve"]:
            await self.nodes[k].set_writable(True)

        # Setpoints
        controls = await plant.add_object(idx, "SetPoints")
        self.nodes["temperature_sp"] = await controls.add_variable(idx, "Max. Temperature", 55.0)
        
        for k in ["temperature_sp"]:
            await self.nodes[k].set_writable(True)
    
    async def sim_loop(self, emit_logs=False):
        logger.info(f"Starting Desinfection PLC simulation...")

        # Create  while loop with simulated values
        counter = 0
        while True:
            # Simulation
            try:
                counter += 1
                self.heater_on = await self.nodes["heater"].read_value()
                self.valve_open = await self.nodes["valve"].read_value()
                
                if self.heater_on:
                    temp_setpoint = await self.nodes["temperature_sp"].read_value()
                    self.temperature = min(temp_setpoint, self.temperature * 1.21)
                else:
                    self.temperature = max(10.0, self.temperature - 7.34)
                
                if self.valve_open:
                    self.tank_pressure = min(10.0, self.tank_pressure + 0.4)
                else:
                    self.tank_pressure = max(0.0, self.tank_pressure - 0.05)

                # Assign values to all the sensors/statuses continuously if constrols allow
                await self.nodes["temperature"].write_value(self.temperature)
                await self.nodes["pressure"].write_value(self.tank_pressure)

                await asyncio.sleep(1)
        
                # Log progress each N seconds
                if self.emit_logs & (counter % 5 == 0) & (counter != 0):
                    logger.info(f"""
    ========= STATUS =========
    --> Heater On: {self.heater_on}
    --> Valve Open: {self.valve_open}

    --> Water Temperature: {self.temperature:.1f}
    --> Tank Pressure: {self.tank_pressure:.2f}
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
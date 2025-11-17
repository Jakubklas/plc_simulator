import asyncio
from asyncua import Server
from asyncua.common.node import Node
import random
import sys
import logging
import time
from threading import Thread

from config import *


logger = logging.getLogger(__name__)

# Disable asyncua verbose logging
logging.getLogger("asyncua").setLevel(logging.WARNING)
logging.getLogger("asyncua.address_space").setLevel(logging.WARNING)
logging.getLogger("asyncua.internal_server").setLevel(logging.WARNING)
logging.getLogger("asyncua.internal_session").setLevel(logging.WARNING)


class WaterPlantPLC:
    def __init__(self, endpoint, uri, name) -> None:
        self.server = Server()
        self.running = True

        self.endpoint = endpoint
        self.uri = uri
        self.name = name

        # Initialize the variables to some numbers
        # Sensors
        self.temp = 0.0
        self.pressure = 0.0
        self.flow = 0.0
        self.ph = 0.0
        # States
        self.pump_on = False
        self.valve_open = False
        self.heater_on = False
        # Nodes dict (comes useful when assignong nodes in bulk)
        self.nodes = {}

    async def start_server(self):
        # Set up the server endpoint (url and name)
        try:
            await self.server.init()
            self.server.set_endpoint(self.endpoint)
            self.server.set_server_name(self.name)
            logger.info(f"Started up the sever {self.server.name} at {self.server.endpoint}")

            # Setup the server http namespace (uri + index)
            uri = "http://waterplant.example.com"
            idx = await self.server.register_namespace(uri)
            logger.info(f"Registered namespace index {self.server.get_namespace_index} (URI: {uri})")

        except Exception as e:
            logger.error(f"Error occured during server start: {e}")

        # Create the server tree root
        try:
            objects = self.server.get_objects_node()

            # Add objects to the node, sensor variables
            plant = await objects.add_object(idx, "WaterPlant")
            logger.info(f"Initialized {plant}")
        
        except Exception as e:
            logger.error(f"Error occured during sensor readings init: {e}")

        # Read-only Sensors
        try:
            sensors = await plant.add_object(idx, "Sensors")

            self.nodes["temp"] = await sensors.add_variable(idx, "Temperature", 0.0)        # Variables always floats (no scaling)
            self.nodes["pressure"] = await sensors.add_variable(idx, "InletPressure", 0.0)
            self.nodes["flow"] = await sensors.add_variable(idx, "FlowRate", 0.0)
            self.nodes["ph"] = await sensors.add_variable(idx, "PhRatio", 0.0)

            for k in ["temp", "pressure", "flow", "ph"]:
                await self.nodes[k].set_writable(False)
                logger.info(f"Initialized sensor reading {k}: {self.nodes[k]}")

        except Exception as e:
            pass

        # Read-Write Controls
        try:
            controls = await plant.add_object(idx, "Controls")
            logger.info(f"Initialized {plant}")

            self.nodes["pump"] = await controls.add_variable(idx, "PumpControl", False)
            self.nodes["valve"] = await controls.add_variable(idx, "ValveControl", False)
            self.nodes["heater"] = await controls.add_variable(idx, "HeaterControl", False)

            for k in ["pump", "valve", "heater"]:
                await self.nodes[k].set_writable(True)
                logger.info(f"Initialized control {k}: {self.nodes[k]}")
        
        except Exception as e:
            logger.error(f"Error occured during controls init: {e}")

        # Read-only status
        try:
            status = await plant.add_object(idx, "Status")
            self.nodes["pump_on"] = await status.add_variable(idx, "PumpRunning", False)
            self.nodes["valve_open"] = await status.add_variable(idx, "ValveOpen", False)
            self.nodes["heater_on"] = await status.add_variable(idx, "HeaterOn", False)
            self.nodes["flow_alarm"] = await status.add_variable(idx, "FlowAlarm", False)
            self.nodes["temp_alarm"] = await status.add_variable(idx, "TempAlarm", False)

            for k in ["pump_on", "valve_open", "heater_on"]:
                await self.nodes[k].set_writable(False)
                logger.info(f"Initialized status {k}: {self.nodes[k]}")

        except Exception as e:
            logger.error(f"Error occured during status init: {e}")

        # Read-Write Setpoints (Holding Registers = PLC input variables)
        try:
            setpoints = await plant.add_object(idx, "SetPoints")
            self.nodes["temp_sp"] = await setpoints.add_variable(idx, "TemperatureSetpoint", 25.0)
            self.nodes["pressure_sp"] = await setpoints.add_variable(idx, "InletPressureSetpoint", 3.5)
            self.nodes["flow_sp"] = await setpoints.add_variable(idx, "FlowRateSetpoint", 150.0)

            for k in ["temp_sp", "pressure_sp", "flow_sp"]:
                await self.nodes[k].set_writable(True)
                logger.info(f"Initialized setpoint {k}: {self.nodes[k]}")
        
        except Exception as e:
            logger.error(f"Error occured during setpoints init: {e}")
        
        logger.info("Printing the OPC UA tree...")
        try:
            await self._view_tree(uri, idx)
        except Exception as e:
            logger.error(f"Error occured during printing the server tree: {e}")


    async def _view_tree(self, uri, idx):
        print("\n" + "="*70)
        print("WATER TREATMENT PLANT OPC UA SERVER")
        print("="*70)
        print(f"Endpoint: opc.tcp://0.0.0.0:4840/freeopcua/server/")
        print(f"Namespace: {uri}")
        print(f"Namespace Index: {idx}")
        print("\nNode Structure:")
        print("  Objects/")
        print("    └─ WaterPlant/")
        print("       ├─ Sensors/ (read-only)")
        print("       │  ├─ Temperature")
        print("       │  ├─ InletPressure")
        print("       │  ├─ OutletPressure")
        print("       │  ├─ FlowRate")
        print("       │  └─ PhLevel")
        print("       ├─ Controls/ (read-write)")
        print("       │  ├─ PumpControl")
        print("       │  ├─ ValveControl")
        print("       │  └─ HeaterControl")
        print("       ├─ Status/ (read-only)")
        print("       │  ├─ PumpRunning")
        print("       │  ├─ ValveOpen")
        print("       │  ├─ HeaterOn")
        print("       │  ├─ HighFlowAlarm")
        print("       │  └─ HighTempAlarm")
        print("       └─ Setpoints/ (read-write)")
        print("          ├─ TemperatureSetpoint")
        print("          ├─ PressureSetpoint")
        print("          └─ FlowSetpoint")
        print("="*70)
        print("\nServer is running. Press Ctrl+C to stop.\n")


    async def _sim_loop(self):
        """
        Continuously writes values to variables if self.running == True
        """
        counter = 0 
        while self.running:
            try:
                # Reading controls commmands from the client
                self.pump_on = await self.nodes["pump"].read_value()
                self.valve_open = await self.nodes["valve"].read_value()
                self.heater_on = await self.nodes["heater"].read_value()
            
            except Exception as e:
                logger.error(f"Error occured during simulation --> reading controls: {e}")

            try:
                # Simulating bahaviour
                if self.pump_on:
                    self.flow = min(200.0, self.flow + random.uniform(-5.0, 10.0))
                    self.pressure = 3.5 + random.uniform(-0.2, 0.3)
                else:
                    self.flow = max(0.0, self.flow - 15.0)
                    self.pressure = 1.0 + random.uniform(-0.1, 0.1)
                
                if self.valve_open:
                    self.pressure = self.pressure * 0.85
                else:
                    self.pressure = self.pressure * 0.3
                
                if self.heater_on:
                    self.temp = min(35.0, self.temp + 0.3)
                else:
                    self.temp = max(18.0, self.temp - 0.1)
                
                self.temp += random.uniform(-0.2, 0.2)
                self.ph = 7.2 + random.uniform(-0.3, 0.3)
            
            except Exception as e:
                logger.error(f"Error occured during simulation --> simulating values: {e}")

            try:
                # Write the simulated numbers into the server node variables
                await self.nodes["temp"].write_value(self.temp)
                await self.nodes["flow"].write_value(self.flow)
                await self.nodes["ph"].write_value(self.ph)
                await self.nodes["pressure"].write_value(self.pressure)

                # Write the recent controls into the server node variables
                await self.nodes['pump'].write_value(self.pump_on)
                await self.nodes['valve'].write_value(self.valve_open)
                await self.nodes['heater'].write_value(self.heater_on)
                await self.nodes['flow_alarm'].write_value(self.flow > 180.0)
                await self.nodes['temp_alarm'].write_value(self.temp > 30.0)

            except Exception as e:
                logger.error(f"Error occured during simulation --> writing values: {e}")

            try:
                # Report on progress
                counter += 1
                if (counter > 0) & (counter % 5 == 0):
                    print()
                    print()
                    logger.info(" --> LATEST STATS:")
                    for k in self.nodes:
                        val = await self.nodes[k].read_value()
                        logger.info(f" --> {k}: {(round(val, 2) if type(val) == float else val)}")
                    counter = 0

                # Sleep before writing new values
                await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("Simulation loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error occured during simulation --> counter: {e}")

    async def run(self):
        """
        Orchestrates the class by creating a server &
        tunning the simulation loop asynchronously.
        """
        await self.start_server()
        
        try:
            await self.server.start()
            await self._sim_loop()
        except KeyboardInterrupt:
            logger.info("Shutting down server gracefully.")
            await self.server.stop()
        except Exception as e:
            logger.info("Shutting down server gracefully.")
            await self.server.stop()
            
            
if __name__ == "__main__":
    plc = WaterPlantPLC(ENDPOINT, URI, NAME)
    thread = Thread(target=lambda: asyncio.run(plc.run()), daemon=False)
    thread.start()
    time.sleep(3)
    
    # Launch the OPC UA client in background
    import subprocess
    subprocess.Popen(["opcua-client", ENDPOINT])
    
    # Keep main process alive - server continues running
    try:
        thread.join()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        plc.running = False
        thread.join()



        







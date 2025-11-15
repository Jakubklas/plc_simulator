"""
OPC UA Water Treatment Plant PLC Simulator

Key differences from Modbus:
- Native float/bool support (no scaling needed!)
- Hierarchical structure (organize variables in folders)
- Built-in discovery (browse server structure)
- Security options (encryption, authentication)
"""

import time
import random
import asyncio
from asyncua import Server
from asyncua.common.node import Node


class WaterPlantOPCUA:
    """
    OPC UA server simulating a water treatment plant
    Much cleaner than Modbus - no register addresses or scaling!
    """
    
    def __init__(self):
        self.server = Server()
        self.running = True
        
        # Process variables (using actual float values, not scaled integers!)
        self.tank_temp = 22.5           # °C
        self.inlet_pressure = 3.5       # bar
        self.outlet_pressure = 2.8      # bar
        self.flow_rate = 150.0          # L/min
        self.ph_level = 7.2             # pH
        
        # Control states
        self.pump_running = False
        self.valve_open = False
        self.heater_on = False
        
        # OPC UA node references (will be set during setup)
        self.nodes = {}
    
    
    async def setup_server(self):
        """
        Configure OPC UA server and create address space
        
        OPC UA uses a hierarchical "address space" instead of flat register addresses
        This makes it much easier to organize and discover data!
        """
        
        # Basic server setup
        await self.server.init()
        self.server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")        #TODO: How to read & understand this endpoint? esp. the freeopcua/server
        self.server.set_server_name("Water Treatment Plant PLC")
        
        # Set up namespace (like a folder for our variables)
        uri = "http://waterplant.example.com"                                       #TODO: What's a namespace really? A an internet domain to run under behind a DNS?
        idx = await self.server.register_namespace(uri)
        
        # Get the Objects node (root of the tree)
        objects = self.server.get_objects_node()                                    #TODO: Show a visual representation of the tree structure
        
        # Create object structure - notice how organized this is vs Modbus registers!
        # Objects > WaterPlant > Sensors, Controls, Setpoints
        plant = await objects.add_object(idx, "WaterPlant")
        
        # === SENSORS (Read-only) ===
        sensors = await plant.add_object(idx, "Sensors")                                    #TODO: What are all these awaits for?
        self.nodes['temp'] = await sensors.add_variable(idx, "Temperature", 0.0)
        self.nodes['inlet_p'] = await sensors.add_variable(idx, "InletPressure", 0.0)
        self.nodes['outlet_p'] = await sensors.add_variable(idx, "OutletPressure", 0.0)
        self.nodes['flow'] = await sensors.add_variable(idx, "FlowRate", 0.0)
        self.nodes['ph'] = await sensors.add_variable(idx, "PhLevel", 0.0)
        
        # Make sensors read-only (important!)
        for key in ['temp', 'inlet_p', 'outlet_p', 'flow', 'ph']:
            await self.nodes[key].set_writable(False)                                           #TODO: .set_writeable(False) clearly makes a variable read-only or read/write.
        
        # === CONTROLS (Read-write) ===
        controls = await plant.add_object(idx, "Controls")                                      #TODO: Seems that 'objects' are for organising data & 'variables' are for storing data
        self.nodes['pump'] = await controls.add_variable(idx, "PumpControl", False)
        self.nodes['valve'] = await controls.add_variable(idx, "ValveControl", False)
        self.nodes['heater'] = await controls.add_variable(idx, "HeaterControl", False)
        
        # Make controls writable
        for key in ['pump', 'valve', 'heater']:
            await self.nodes[key].set_writable(True) 
        
        # === STATUS (Read-only) ===
        status = await plant.add_object(idx, "Status")
        self.nodes['pump_status'] = await status.add_variable(idx, "PumpRunning", False)
        self.nodes['valve_status'] = await status.add_variable(idx, "ValveOpen", False)
        self.nodes['heater_status'] = await status.add_variable(idx, "HeaterOn", False)
        self.nodes['flow_alarm'] = await status.add_variable(idx, "HighFlowAlarm", False)
        self.nodes['temp_alarm'] = await status.add_variable(idx, "HighTempAlarm", False)
        
        for key in ['pump_status', 'valve_status', 'heater_status', 'flow_alarm', 'temp_alarm']:
            await self.nodes[key].set_writable(False)
        
        # === SETPOINTS (Read-write) ===
        setpoints = await plant.add_object(idx, "Setpoints")
        self.nodes['temp_sp'] = await setpoints.add_variable(idx, "TemperatureSetpoint", 25.0)
        self.nodes['pressure_sp'] = await setpoints.add_variable(idx, "PressureSetpoint", 3.5)
        self.nodes['flow_sp'] = await setpoints.add_variable(idx, "FlowSetpoint", 150.0)
        
        await self.nodes['temp_sp'].set_writable(True)
        await self.nodes['pressure_sp'].set_writable(True)
        await self.nodes['flow_sp'].set_writable(True)
        
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
    
    
    async def simulation_loop(self):
        """
        Main simulation loop - updates sensor values based on control states
        Similar logic to Modbus version but cleaner data access!
        """
        print("Starting simulation loop...\n")
        
        while self.running:
            try:
                # Read control commands from clients
                # Notice: no device_id, function codes, or address offsets!
                self.pump_running = await self.nodes['pump'].read_value()          #TODO: Seems as though OPCUA does not strictly separate data into ir, hr, di and co like Modbus. Instead it's a user-defined folder system of varables
                self.valve_open = await self.nodes['valve'].read_value()
                self.heater_on = await self.nodes['heater'].read_value()
                
                # Simulate realistic process behavior
                if self.pump_running:
                    self.flow_rate = min(200.0, self.flow_rate + random.uniform(-5, 10))
                    self.inlet_pressure = 3.5 + random.uniform(-0.2, 0.3)
                else:
                    self.flow_rate = max(0.0, self.flow_rate - 15)
                    self.inlet_pressure = 1.0 + random.uniform(-0.1, 0.1)
                
                if self.valve_open:
                    self.outlet_pressure = self.inlet_pressure * 0.85
                else:
                    self.outlet_pressure = self.inlet_pressure * 0.3
                
                if self.heater_on:
                    self.tank_temp = min(35.0, self.tank_temp + 0.3)
                else:
                    self.tank_temp = max(18.0, self.tank_temp - 0.1)
                
                # Add realistic noise
                self.tank_temp += random.uniform(-0.2, 0.2)
                self.ph_level = 7.2 + random.uniform(-0.3, 0.3)
                
                # Write sensor values - notice: actual floats, no scaling!
                await self.nodes['temp'].write_value(self.tank_temp)                        #TODO: Super simple data access: just node.read_value() or node.write_vlaue() and node.set_writeable(bool)
                await self.nodes['inlet_p'].write_value(self.inlet_pressure)
                await self.nodes['outlet_p'].write_value(self.outlet_pressure)
                await self.nodes['flow'].write_value(self.flow_rate)
                await self.nodes['ph'].write_value(self.ph_level)
                
                # Update status
                await self.nodes['pump_status'].write_value(self.pump_running)
                await self.nodes['valve_status'].write_value(self.valve_open)
                await self.nodes['heater_status'].write_value(self.heater_on)
                await self.nodes['flow_alarm'].write_value(self.flow_rate > 180)
                await self.nodes['temp_alarm'].write_value(self.tank_temp > 30)
                
                await asyncio.sleep(1)
                
            except Exception as e:
                print(f"Error in simulation loop: {e}")
                await asyncio.sleep(1)
    
    
    async def run(self):
        """
        Start the OPC UA server
        """
        await self.setup_server()
        
        async with self.server:                             #TODO: Why context manager? How does this work or help?
            # Run simulation
            await self.simulation_loop()


async def main():
    plant = WaterPlantOPCUA()
    
    try:
        await plant.run()
    except KeyboardInterrupt:
        print("\n\nShutting down server...")
        plant.running = False


if __name__ == "__main__":
    asyncio.run(main())
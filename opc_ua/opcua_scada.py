import asyncio
from asyncua import Client
from datetime import datetime
from config import *

class WaterPlantSCADA:
    """
    SCADA interface using OPC UA instead of Modbus
    Much simpler and more intuitive!
    """
    
    def __init__(self, endpoint, uri):
        self.endpoint = endpoint
        self.uri = uri
        self.client = None
        self.nodes = {}
    
    async def connect(self):
        """
        Connect to OPC UA server
        """
        print(f"Connecting to OPC UA server at {self.endpoint}...")
        
        self.client = Client(url=self.endpoint)
        
        try:
            await self.client.connect()
            print("✓ Connected successfully!\n")
            
            # Discover and cache node references
            # This is like a "map" of what's available on the server
            await self.discover_nodes()
            
            return True
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            return False
    
    
    async def discover_nodes(self):
        """
        Browse server structure and cache node references
        
        This is a HUGE advantage over Modbus - you can explore what's available!
        In Modbus you need documentation to know register addresses.
        """
        print("Discovering server nodes...")
        
        # Get namespace index (usually 2 for custom namespaces)
        nsidx = await self.client.get_namespace_index(self.uri)
        
        # Navigate to our plant object
        root = self.client.get_objects_node()
        
        # Get WaterPlant object
        plant = await root.get_child([f"{nsidx}:WaterPlant"])
        
        # Get all sub-objects
        sensors = await plant.get_child([f"{nsidx}:Sensors"])
        controls = await plant.get_child([f"{nsidx}:Controls"])
        status = await plant.get_child([f"{nsidx}:Status"])
        setpoints = await plant.get_child([f"{nsidx}:Setpoints"])
        
        # Cache sensor nodes
        self.nodes['temp'] = await sensors.get_child([f"{nsidx}:Temperature"])
        self.nodes['inlet_p'] = await sensors.get_child([f"{nsidx}:InletPressure"])
        self.nodes['outlet_p'] = await sensors.get_child([f"{nsidx}:OutletPressure"])
        self.nodes['flow'] = await sensors.get_child([f"{nsidx}:FlowRate"])
        self.nodes['ph'] = await sensors.get_child([f"{nsidx}:PhLevel"])
        
        # Cache control nodes
        self.nodes['pump'] = await controls.get_child([f"{nsidx}:PumpControl"])
        self.nodes['valve'] = await controls.get_child([f"{nsidx}:ValveControl"])
        self.nodes['heater'] = await controls.get_child([f"{nsidx}:HeaterControl"])
        
        # Cache status nodes
        self.nodes['pump_status'] = await status.get_child([f"{nsidx}:PumpRunning"])
        self.nodes['valve_status'] = await status.get_child([f"{nsidx}:ValveOpen"])
        self.nodes['heater_status'] = await status.get_child([f"{nsidx}:HeaterOn"])
        self.nodes['flow_alarm'] = await status.get_child([f"{nsidx}:HighFlowAlarm"])
        self.nodes['temp_alarm'] = await status.get_child([f"{nsidx}:HighTempAlarm"])
        
        # Cache setpoint nodes
        self.nodes['temp_sp'] = await setpoints.get_child([f"{nsidx}:TemperatureSetpoint"])
        self.nodes['pressure_sp'] = await setpoints.get_child([f"{nsidx}:PressureSetpoint"])
        self.nodes['flow_sp'] = await setpoints.get_child([f"{nsidx}:FlowSetpoint"])
        
        print(f"✓ Discovered {len(self.nodes)} nodes\n")
    
    
    async def disconnect(self):
        """
        Disconnect from server
        """
        if self.client:
            await self.client.disconnect()
            print("\nDisconnected from OPC UA server")
    
    
    async def read_sensors(self):
        """
        Read all sensor values
        Notice: No scaling needed! OPC UA supports native floats.
        """
        try:
            sensors = {
                'temperature': await self.nodes['temp'].read_value(),
                'inlet_pressure': await self.nodes['inlet_p'].read_value(),
                'outlet_pressure': await self.nodes['outlet_p'].read_value(),
                'flow_rate': await self.nodes['flow'].read_value(),
                'ph_level': await self.nodes['ph'].read_value()
            }
            return sensors
        except Exception as e:
            print(f"Error reading sensors: {e}")
            return None
    
    
    async def read_status(self):
        """
        Read equipment status
        """
        try:
            status = {
                'pump_running': await self.nodes['pump_status'].read_value(),
                'valve_open': await self.nodes['valve_status'].read_value(),
                'heater_on': await self.nodes['heater_status'].read_value(),
                'flow_alarm': await self.nodes['flow_alarm'].read_value(),
                'temp_alarm': await self.nodes['temp_alarm'].read_value()
            }
            return status
        except Exception as e:
            print(f"Error reading status: {e}")
            return None
    
    
    async def read_setpoints(self):
        """
        Read current setpoints
        """
        try:
            setpoints = {
                'temp_setpoint': await self.nodes['temp_sp'].read_value(),
                'pressure_setpoint': await self.nodes['pressure_sp'].read_value(),
                'flow_setpoint': await self.nodes['flow_sp'].read_value()
            }
            return setpoints
        except Exception as e:
            print(f"Error reading setpoints: {e}")
            return None
    
    
    async def control_pump(self, state):
        """
        Control pump - notice how much simpler than Modbus!
        No coil addresses, no device IDs
        """
        try:
            await self.nodes['pump'].write_value(state)
            state_str = "ON" if state else "OFF"
            print(f"✓ Pump turned {state_str}")
            return True
        except Exception as e:
            print(f"Error controlling pump: {e}")
            return False
    
    
    async def control_valve(self, state):
        """
        Control valve
        """
        try:
            await self.nodes['valve'].write_value(state)
            state_str = "OPEN" if state else "CLOSED"
            print(f"✓ Valve {state_str}")
            return True
        except Exception as e:
            print(f"Error controlling valve: {e}")
            return False
    
    
    async def control_heater(self, state):
        """
        Control heater
        """
        try:
            await self.nodes['heater'].write_value(state)
            state_str = "ON" if state else "OFF"
            print(f"✓ Heater turned {state_str}")
            return True
        except Exception as e:
            print(f"Error controlling heater: {e}")
            return False
    
    
    async def write_setpoint(self, setpoint_name, value):
        """
        Write a setpoint value
        """
        try:
            await self.nodes[setpoint_name].write_value(float(value))
            print(f"✓ {setpoint_name} set to {value}")
            return True
        except Exception as e:
            print(f"Error writing setpoint: {e}")
            return False
    
    
    async def display_dashboard(self):
        """
        Display real-time plant status
        """
        print("\n" + "="*70)
        print("WATER TREATMENT PLANT - LIVE DASHBOARD (OPC UA)")
        print("="*70)
        
        sensors = await self.read_sensors()
        status = await self.read_status()
        setpoints = await self.read_setpoints()
        
        if not sensors or not status:
            print("Error reading data from server")
            return
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Time: {timestamp}\n")
        
        # Sensor readings - notice: actual float values, no conversion!
        print("SENSOR READINGS:")
        print(f"  Tank Temperature:  {sensors['temperature']:6.2f} °C")
        print(f"  Inlet Pressure:    {sensors['inlet_pressure']:6.2f} bar")
        print(f"  Outlet Pressure:   {sensors['outlet_pressure']:6.2f} bar")
        print(f"  Flow Rate:         {sensors['flow_rate']:6.1f} L/min")
        print(f"  pH Level:          {sensors['ph_level']:6.2f}")
        
        # Equipment status
        print("\nEQUIPMENT STATUS:")
        print(f"  Pump:    {'🟢 RUNNING' if status['pump_running'] else '🔴 STOPPED'}")
        print(f"  Valve:   {'🟢 OPEN' if status['valve_open'] else '🔴 CLOSED'}")
        print(f"  Heater:  {'🟢 ON' if status['heater_on'] else '🔴 OFF'}")
        
        # Alarms
        print("\nALARMS:")
        alarms = []
        if status['temp_alarm']:
            alarms.append("⚠️  HIGH TEMPERATURE")
        if status['flow_alarm']:
            alarms.append("⚠️  HIGH FLOW RATE")
        
        if alarms:
            for alarm in alarms:
                print(f"  {alarm}")
        else:
            print("  ✓ All systems normal")
        
        # Setpoints
        if setpoints:
            print("\nSETPOINTS:")
            print(f"  Temperature Target: {setpoints['temp_setpoint']:.1f} °C")
            print(f"  Pressure Target:    {setpoints['pressure_setpoint']:.1f} bar")
            print(f"  Flow Target:        {setpoints['flow_setpoint']:.1f} L/min")
        
        print("="*70)
    
    
    async def browse_server(self):
        """
        Browse and print server structure
        
        This is unique to OPC UA - you can discover what's available!
        With Modbus you need documentation.
        """
        print("\n" + "="*70)
        print("BROWSING SERVER STRUCTURE")
        print("="*70 + "\n")
        
        async def print_node(node, indent=0):
            """Recursively print node tree"""
            try:
                name = await node.read_browse_name()
                value = None
                try:
                    value = await node.read_value()
                except:
                    pass  # Not all nodes have values
                
                prefix = "  " * indent
                if value is not None:
                    print(f"{prefix}├─ {name.Name} = {value}")
                else:
                    print(f"{prefix}├─ {name.Name}/")
                
                children = await node.get_children()
                for child in children:
                    await print_node(child, indent + 1)
            except Exception as e:
                pass
        
        root = self.client.get_objects_node()
        await print_node(root)
        print()


async def interactive_mode(scada):
    """
    Interactive control mode
    """
    print("\n" + "="*70)
    print("INTERACTIVE CONTROL MODE")
    print("="*70)
    print("\nCommands:")
    print("  1  - Turn pump ON")
    print("  0  - Turn pump OFF")
    print("  3  - Open valve")
    print("  2  - Close valve")
    print("  5  - Turn heater ON")
    print("  4  - Turn heater OFF")
    print("  d  - Display dashboard")
    print("  m  - Monitor mode (continuous updates)")
    print("  s  - Set temperature setpoint")
    print("  b  - Browse server structure")
    print("  q  - Quit")
    print("="*70 + "\n")
    
    while True:
        try:
            cmd = input("Command: ").strip().lower()
            
            if cmd == '1':
                await scada.control_pump(True)
            elif cmd == '0':
                await scada.control_pump(False)
            elif cmd == '3':
                await scada.control_valve(True)
            elif cmd == '2':
                await scada.control_valve(False)
            elif cmd == '5':
                await scada.control_heater(True)
            elif cmd == '4':
                await scada.control_heater(False)
            elif cmd == 'd':
                await scada.display_dashboard()
            elif cmd == 'm':
                print("\nMonitoring mode - Press Ctrl+C to stop\n")
                try:
                    while True:
                        await scada.display_dashboard()
                        await asyncio.sleep(2)
                except KeyboardInterrupt:
                    print("\nMonitoring stopped\n")
            elif cmd == 's':
                try:
                    temp = float(input("Enter temperature setpoint (°C): "))
                    await scada.write_setpoint('temp_sp', temp)
                except ValueError:
                    print("Invalid temperature value")
            elif cmd == 'b':
                await scada.browse_server()
            elif cmd == 'q':
                break
            else:
                print("Unknown command")
                
        except KeyboardInterrupt:
            print("\n")
            break
        except EOFError:
            break


async def main():
    """
    Main program
    """
    import sys
    

    if len(sys.argv) < 2:
        print("Usage: python3 opcua_client.py <SERVER_IP>")
        print("Example: python3 opcua_client.py 192.168.1.100")
        print("         python3 opcua_client.py localhost")
        sys.exit(1)
    
    server_ip = sys.argv[1]
    endpoint = f"opc.tcp://{server_ip}:4840/freeopcua/server/"
    
    scada = WaterPlantSCADA(ENDPOINT, URI)
    
    if not await scada.connect():
        sys.exit(1)
    
    try:
        # Show initial dashboard
        await scada.display_dashboard()
        
        # Enter interactive mode
        await interactive_mode(scada)
        
    finally:
        await scada.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
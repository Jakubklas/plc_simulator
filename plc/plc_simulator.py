import time
import random
import threading
from pymodbus.server import StartTcpServer
from pymodbus.datastore import ModbusDeviceContext, ModbusServerContext
from pymodbus.datastore import ModbusSequentialDataBlock

class WaterPlantSimulator:
    """
    Simulates a water treatment plant with realistic sensor data
    """
    def __init__(self, context):
        self.context = context
        self.running = True
        
        # Process variables (realistic ranges)
        self.tank_temp = 22.5           # °C
        self.inlet_pressure = 3.5       # bar
        self.outlet_pressure = 2.8      # bar
        self.flow_rate = 150            # L/min
        self.ph_level = 7.2             # Ph ratio
        
        # Control states
        self.pump_running = False
        self.valve_open = False
        self.heater_on = False
        

    def start_simulation(self):
        """
        Start the background simulation thread
        """
        thread = threading.Thread(target=self._simulation_loop, daemon=True)
        thread.start()
        

    def _simulation_loop(self):
        """Main simulation loop - updates sensor values"""
        print("Starting water plant simulation...")
        
        while self.running:
            # Read control states from coils (addresses 0-9)
            slave_id = 0x00
            coils = self.context[slave_id].getValues(1, 0, count=10)
            
            self.pump_running = bool(coils[0])
            self.valve_open = bool(coils[1])
            self.heater_on = bool(coils[2])
            
            # Simulate realistic process behavior
            if self.pump_running:
                self.flow_rate = min(200, self.flow_rate + random.uniform(-5, 10))
                self.inlet_pressure = 3.5 + random.uniform(-0.2, 0.3)
            else:
                self.flow_rate = max(0, self.flow_rate - 15)
                self.inlet_pressure = 1.0 + random.uniform(-0.1, 0.1)
            
            if self.valve_open:
                self.outlet_pressure = self.inlet_pressure * 0.85
            else:
                self.outlet_pressure = self.inlet_pressure * 0.3
            
            if self.heater_on:
                self.tank_temp = min(35, self.tank_temp + 0.3)
            else:
                self.tank_temp = max(18, self.tank_temp - 0.1)
            
            # Add some realistic noise
            self.tank_temp += random.uniform(-0.2, 0.2)
            self.ph_level = 7.2 + random.uniform(-0.3, 0.3)
            
            # Write sensor values to input registers (addresses 0-9)
            # Scale values to integers (typical for PLCs)
            registers = [
                int(self.tank_temp * 100),          # Reg 0: Temperature (scaled x100)
                int(self.inlet_pressure * 100),     # Reg 1: Inlet pressure (scaled x100)
                int(self.outlet_pressure * 100),    # Reg 2: Outlet pressure (scaled x100)
                int(self.flow_rate * 10),           # Reg 3: Flow rate (scaled x10)
                int(self.ph_level * 100),           # Reg 4: pH level (scaled x100)
                int(time.time() % 65535),           # Reg 5: Timestamp (seconds mod 65535)
                0,                                  # Reg 6-9: Reserved
                0,
                0,
                0,
            ]
            
            # Update input registers
            #                                4 = Function code for Input Registers
            #                                |  0 = Starting at the address zero
            #                                |  |      Values to write as a list
            #                                |  |      |
            #                                v  v      v
            self.context[slave_id].setValues(4, 0, registers)
            
            # Write status to discrete inputs (addresses 0-9)
            discrete_inputs = [
                self.pump_running,
                self.valve_open,
                self.heater_on,
                self.flow_rate > 50,            # Flow sensor high
                self.tank_temp > 30,            # Temperature alarm
                self.inlet_pressure > 4.0,      # Pressure alarm
                False,                          # Emergency stop (not activated)
                True,                           # System healthy
                False,                          # Reserved
                False,                          # Reserved
            ]
            
            # Update discrete inputs
            self.context[slave_id].setValues(2, 0, discrete_inputs)
            
            # Also write current setpoints to holding registers (addresses 0-9)
            # These can be read and written by the client
            holding_regs = [
                int(25.0 * 100),  # Temperature setpoint
                int(3.5 * 100),   # Pressure setpoint
                int(150 * 10),    # Flow rate setpoint
                0, 0, 0, 0, 0, 0, 0
            ]
            self.context[slave_id].setValues(3, 0, holding_regs)
            
            time.sleep(1)  # Update every second


def run_server():
    """
    Initialize and run the Modbus TCP server
    """
    
    # Initialize data blocks
    # Modbus function codes: 1=Coils, 2=Discrete Inputs, 3=Holding Registers, 4=Input Registers
    store = ModbusDeviceContext(
        co=ModbusSequentialDataBlock(0, [0]*100),  # Coils (digital outputs)
        di=ModbusSequentialDataBlock(0, [0]*100),  # Discrete Inputs (digital inputs)
        hr=ModbusSequentialDataBlock(0, [0]*100),  # Holding Registers (read/write analog)
        ir=ModbusSequentialDataBlock(0, [0]*100)   # Input Registers (read-only analog)
    )
    
    context = ModbusServerContext(devices=store, single=True)
    
    # Start the simulation
    simulator = WaterPlantSimulator(context)
    simulator.start_simulation()
    
    # Get the Pi's IP address
    import socket
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    
    print("="*60)
    print("WATER TREATMENT PLANT PLC SIMULATOR")
    print("="*60)
    print(f"Server starting on {local_ip}:502")
    print("\nModbus Register Map:")
    print("-" * 60)
    print("COILS (Digital Outputs, Read/Write):")
    print("  Address 0: Pump Control (0=OFF, 1=ON)")
    print("  Address 1: Valve Control (0=CLOSED, 1=OPEN)")
    print("  Address 2: Heater Control (0=OFF, 1=ON)")
    print("\nDISCRETE INPUTS (Digital Inputs, Read-Only):")
    print("  Address 0: Pump Status")
    print("  Address 1: Valve Status")
    print("  Address 2: Heater Status")
    print("  Address 3: Flow Sensor High")
    print("  Address 4: Temperature Alarm")
    print("  Address 5: Pressure Alarm")
    print("  Address 7: System Healthy")
    print("\nINPUT REGISTERS (Analog Inputs, Read-Only):")
    print("  Address 0: Tank Temperature (°C x100)")
    print("  Address 1: Inlet Pressure (bar x100)")
    print("  Address 2: Outlet Pressure (bar x100)")
    print("  Address 3: Flow Rate (L/min x10)")
    print("  Address 4: pH Level (x100)")
    print("  Address 5: Timestamp")
    print("\nHOLDING REGISTERS (Setpoints, Read/Write):")
    print("  Address 0: Temperature Setpoint (°C x100)")
    print("  Address 1: Pressure Setpoint (bar x100)")
    print("  Address 2: Flow Rate Setpoint (L/min x10)")
    print("="*60)
    print("\nServer is running. Press Ctrl+C to stop.\n")
    
    # Start Modbus TCP server on port 502 (requires root/sudo)
    # If permission denied, try port 5020 instead
    try:
        StartTcpServer(context=context, address=("0.0.0.0", 502))
    except PermissionError:
        print("\nPort 502 requires root privileges. Trying port 5020...")
        print("Note: Update client to connect to port 5020\n")
        StartTcpServer(context=context, address=("0.0.0.0", 5020))


if __name__ == "__main__":
    run_server()
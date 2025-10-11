import time
import random
import threading
from pymodbus.server import StartTcpServer
from pymodbus.datastore import ModbusServerContext, ModbusSequentialDataBlock
from pymodbus.datastore.context import ModbusDeviceContext

class WaterTankPLC:
    def __init__(self, context):
        self.running = True
        self.context = context

        self.flow_rate = 150
        self.inlet_pressure = 315
        self.tank_temp = 22.5

        self.pump_running = False
        self.heater_on = False

    def run_simulation(self):
        thread = threading.Thread(target=self.run_loop, daemon=True)
        thread.start()

    def run_loop(self):
        print(f"Simulation loop started for Water Tank PLC...")
        device = 1
        while self.running:
            # Read/Write coils
            coils = self.context[device].getValues(1, 0, count=10)

            self.pump_running = bool(coils[0])
            self.heater_on = bool(coils[1])

            # Read/Write input registers
            if self.pump_running:
                self.flow_rate = min(200, self.flow_rate + random.uniform(-5, 10))
                self.inlet_pressure = min(250, self.inlet_pressure + random.uniform(-50, 70))
            else:
                self.flow_rate = max(0, self.flow_rate - 15)
                self.inlet_pressure = min(15, self.flow_rate + random.uniform(-10, 0))
            
            if self.heater_on:
                self.tank_temp = min(75, self.tank_temp + 0.3)
            else:
                self.tank_temp = max(18, self.tank_temp - 0.1)
            
            # Update the input registers and discrete inputs
            registers = [
                int(self.flow_rate * 100),
                int(self.tank_temp * 100),
                0,
                0,
                0,
                0,
                0,
                0
            ]
            self.context[device].setValues(4, 0, registers)

            discrete_inputs = [
                self.pump_running,
                self.heater_on,
                0,
                0,
                0,
                0,
                0,
                0
            ]
            self.context[device].setValues(2, 0, discrete_inputs)

            # Write to the Holding registers if needed
            holding_registers = [
                int(25.0 * 100),
                int(3.5 *100),
                int(150 * 10),
                0, 0, 0, 0, 0, 0, 0
            ]
            self.context[device].setValues(3, 0, holding_registers)

            time.sleep(1)

class ConveyotBeltPLC:
    def __init__(self, context):
        self.running = True
        self.context = context

        self.belt_speed = 5
        self.belt_vibration = 13

        self.suspension_on = False
        self.belt_on = False

    def run_simulation(self):
        thread = threading.Thread(target=self.run_loop, daemon=True)
        thread.start()

    def run_loop(self):
        print(f"Simulation loop started for Converyor Belt PLC...")
        device = 2
        while self.running:
            # Read/Write coils
            coils = self.context[device].getValues(1, 0, count=10)

            self.belt_on = bool(coils[0])
            self.suspension_on = bool(coils[1])

            # Read/Write input registers
            if self.suspension_on:
                self.belt_vibration = max(0, min(2, self.belt_vibration + random.uniform(-1, -0.5)))
            else:
                self.belt_vibration = max(5, min(20, self.belt_vibration + random.uniform(-2, 3)))
                
            if self.belt_on:
                self.belt_speed = min(10, self.belt_speed + 0.5)
            else:
                self.belt_speed = max(0, self.belt_speed - 1)
            
            # Update the input registers and discrete inputs
            registers = [
                int(self.belt_speed * 100),
                int(self.belt_vibration * 100),
                0,
                0,
                0,
                0,
                0,
                0
            ]
            self.context[device].setValues(4, 0, registers)

            discrete_inputs = [
                self.belt_on,
                self.suspension_on,
                0,
                0,
                0,
                0,
                0,
                0
            ]
            self.context[device].setValues(2, 0, discrete_inputs)

            # Write to the Holding registers if needed
            holding_registers = [
                int(25.0 * 100),
                int(3.5 *100),
                int(150 * 10),
                0, 0, 0, 0, 0, 0, 0
            ]
            self.context[device].setValues(3, 0, holding_registers)

            time.sleep(1)


def run_server():
    """
    Initializes the store layout (made of data-blocks)
    , and the server iself.
    """
    print("="*60)
    print("PLC MODBUS SERVER - Two Slave Configuration")
    print("="*60)

    # co - Digital True/False outputs (1-bit)
    # di - Analog True/False inputs (1-bit)
    # hr - Digital 16-bit outputs
    # it - Analog 16-bit inputs

    # TODO: How does this memory allocation work
    store1 = ModbusDeviceContext(
        co=ModbusSequentialDataBlock(0, [0]*100),      # Function Code [0]
        di=ModbusSequentialDataBlock(0, [0]*100),      # Function Code [1]
        hr=ModbusSequentialDataBlock(0, [0]*100),      # Function Code [2]
        ir=ModbusSequentialDataBlock(0, [0]*100),      # Function Code [3]
    )
    
    store2 = ModbusDeviceContext(
        co=ModbusSequentialDataBlock(0, [0]*100),      # Function Code [0]
        di=ModbusSequentialDataBlock(0, [0]*100),      # Function Code [1]
        hr=ModbusSequentialDataBlock(0, [0]*100),      # Function Code [2]
        ir=ModbusSequentialDataBlock(0, [0]*100),      # Function Code [3]
    )

    # Establish the memory context & run the simulator
    print("Creating server context...")
    context = ModbusServerContext(devices={1: store1, 2: store2}, single=False)

    print("Starting simulator threads...")
    water_tank_plc = WaterTankPLC(context)
    water_tank_plc.run_simulation()
    conveyor_plc = ConveyotBeltPLC(context)
    conveyor_plc.run_simulation()
    
    # Start the server
    print("\nStarting Modbus TCP Server on 0.0.0.0:502")
    print("\nServer is running. Press Ctrl+C to stop.")
    print("="*60)

    try:
        StartTcpServer(context=context, address=("0.0.0.0", 502))
    except PermissionError:
        print("\nPort 502 requires admin privileges. Trying port 5020...")
        StartTcpServer(context=context, address=("0.0.0.0", 5020))



if __name__ == "__main__":
    run_server()
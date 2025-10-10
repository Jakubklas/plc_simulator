import time
import random
import threading
from pymodbus.server import StartTcpServer
from pymodbus.datastore import ModbusServerContext, ModbusSequentialDataBlock
from pymodbus.datastore.context import ModbusDeviceContext

class PlcSimulator:
    def __init__(self, context):
        self.running = True
        self.context = context

        self.flow_rate = 150
        self.tank_temp = 22.5

        self.pump_running = False
        self.heater_on = False

    def run_simulation(self):
        thread = threading.Thread(target=self.run_loop, daemon=True)
        thread.start()

    def run_loop(self):
        print("Simulation loop started...")
        while self.running:
            for slave in [1, 2]:
                # Read/Write coils
                coils = self.context[slave].getValues(1, 0, count=10)     #TODO: Not sure how this works

                self.pump_running = bool(coils[0])
                self.heater_on = bool(coils[1])

                # Read/Write input registers
                if self.pump_running:
                    self.flow_rate = min(200, self.flow_rate + random.uniform(-5, 10))
                else:
                    self.flow_rate = max(0, self.flow_rate - 15)
                
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
                self.context[slave].setValues(4, 0, registers)

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
                self.context[slave].setValues(2, 0, discrete_inputs)

                # Write to the Holding registers if needed
                holding_registers = [
                    int(25.0 * 100),
                    int(3.5 *100),
                    int(150 * 10),
                    0, 0, 0, 0, 0, 0, 0
                ]
                self.context[slave].setValues(3, 0, holding_registers)

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
    print("Initializing PLC 1...")
    plc_1 = ModbusDeviceContext(
        co=ModbusSequentialDataBlock(0, [0]*100),      # Function Code [0]
        di=ModbusSequentialDataBlock(0, [0]*100),      # Function Code [1]
        hr=ModbusSequentialDataBlock(0, [0]*100),      # Function Code [2]
        ir=ModbusSequentialDataBlock(0, [0]*100),      # Function Code [3]
    )

    print("Initializing PLC 2...")
    plc_2 = ModbusDeviceContext(
        co=ModbusSequentialDataBlock(0, [0]*100),
        di=ModbusSequentialDataBlock(0, [0]*100),
        hr=ModbusSequentialDataBlock(0, [0]*100),
        ir=ModbusSequentialDataBlock(0, [0]*100),
    )

    # Establish the memory context & run the simulator
    print("Creating server context with 2 slaves (IDs: 1, 2)...")
    context = ModbusServerContext(devices={1: plc_1, 2: plc_2}, single=False)

    print("Starting simulator thread...")
    simulator = PlcSimulator(context)
    simulator.run_simulation()

    # Start the server
    print("\nStarting Modbus TCP Server on 0.0.0.0:502")
    print("Slave ID 1: PLC 1")
    print("Slave ID 2: PLC 2")
    print("\nServer is running. Press Ctrl+C to stop.")
    print("="*60)

    try:
        StartTcpServer(context=context, address=("0.0.0.0", 502))
    except PermissionError:
        print("\nPort 502 requires admin privileges. Trying port 5020...")
        StartTcpServer(context=context, address=("0.0.0.0", 5020))



if __name__ == "__main__":
    run_server()
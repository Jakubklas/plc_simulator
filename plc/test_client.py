"""
Simple Modbus client to test the PLC server
"""
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ConnectionException

class Connector:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def connect(self):
        self.client = ModbusTcpClient(host=self.host, port=self.port)
        try:
            self.client.connect()
            return True
        except ConnectionException as e:
            print(f"Failed to connect due to: {e}")
            return False
        
    def read_hr(self, address, count, device):
        try:
            result = self.client.read_holding_registers(address=address, count=count, device_id=device)
            if result.isError():
                print(f"Failed to read holding registers in device {device}: {result}")
                return None
            return result.registers
        except Exception as e:
            print(f"Unexpected error occured: {e}")


# def test_client(self):
#     print("Conneting to Modbus TCP self.client.")
    
#     if self.client.connect():
#         print(f"Successfully connected to TCP self.client over {host}:{port}")
#     else:
#         print(f"Failed to connect.")

#     device_map = {
#         1 : {
#             "co": ["pump_running", "heater_on"],
#             "ir": ["flow_rate", "inlet_pressure", "tank_temp"],
#             "hr": ["flow_rate", "inlet_pressure", "tank_temp"],
#             "di": ["pump_running", "heater_on"],
#             },
#         2 : {
#             "co": ["belt_on", "suspension_on"],
#             "ir": ["belt_speed", "belt_vibration", "_", ],
#             "hr": ["belt_speed", "belt_vibration", "_", ],
#             "di": ["belt_on", "suspension_on"],
#             }
#     }

#     print(f"Reading input registers...")

# while True:
#     for id in [1, 2]:
#         print(f"Readigs from device {id}")
#         ir = self.client.read_input_registers(0, count=10, device_id=id)
#         co = self.client.read_coils(0, count=10, device_id=id)
#         hr = self.client.read_holding_registers(0, count=10, device_id=id)
#         di = self.client.read_discrete_inputs(0, count=10, device_id=id)
#         print(
#             f"""DEVICE {id}:
#             Coils:
#             {device_map[id]["co"][0]}: {bool(co.bits[0])}
#             {device_map[id]["co"][1]}: {bool(co.bits[1])}
#             Input Registers:
#             {device_map[id]["ir"][0]}: {ir.registers[0]/100.0}
#             {device_map[id]["ir"][1]}: {ir.registers[1]/100.0}
#             {device_map[id]["ir"][2]}: {ir.registers[2]/100.0}
#             Holding Registers:
#             {device_map[id]["hr"][0]}: {hr.registers[0]/100.0}
#             {device_map[id]["hr"][1]}: {hr.registers[1]/100.0}
#             {device_map[id]["hr"][2]}: {hr.registers[2]/100.0}
#             Discrete Inputs:
#             {device_map[id]["di"][0]}: {bool(di.bits[0])}
#             {device_map[id]["di"][1]}: {bool(di.bits[1])}
#             """
#         )
#         print()

#     location = input("Write t               o [hr, co]: ")
#     if location not in ["hr", "co"]:
#         print("Incorrect location...\n")
#         continue
#     elif location == "hr":
#         device = input(f"Which device [1, 2]?")
#         register = input(f"Which register [0, 1, 2]?")
#         val = input(f"Value to save [float]")
#         self.client.write_register(address=int(register), value=int(float(val)*100), device_id=int(device))
#     elif location == "co":
#         device = input(f"Which device [1, 2]?")
#         register = input(f"Which register [0, 1]?")
#         val = input(f"Value to save [true/false]")
#         self.client.write_coil(address=int(register), value=val.lower() in ['true', '1', 'yes'], device_id=int(device))


if __name__ == "__main__":
    host="localhost"
    port=502
    connector = Connector(host, port)
    if connector.connect():
        registers = connector.read_hr(
            address=0,
            count=3,
            device=1
        )
        
        if registers:
            pump_pressure, tank_temp, belt_speed = registers
            print(f"Pump pressure: {pump_pressure}")
            print(f"Tank temp: {tank_temp}")
            print(f"Belt speed: {belt_speed}")
        else:
            print("Failed to read holding registers")
                
    else:
        print("Connection problem...!")


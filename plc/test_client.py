"""
Simple Modbus client to test the PLC server
"""
from pymodbus.client import ModbusTcpClient

def test_connection():
    # Connect to the server
    client = ModbusTcpClient('localhost', port=502)

    if client.connect():
        print("✓ Connected to Modbus server on localhost:502")

        # Test reading from slave 1
        print("\n--- Testing Slave ID 1 ---")
        result = client.read_input_registers(address=0, count=2, device_id=1)
        if not result.isError():
            flow_rate = result.registers[0] / 100
            tank_temp = result.registers[1] / 100
            print(f"Flow Rate: {flow_rate} L/min")
            print(f"Tank Temp: {tank_temp} °C")
        else:
            print(f"Error reading from slave 1: {result}")

        # Test reading from slave 2
        print("\n--- Testing Slave ID 2 ---")
        result = client.read_input_registers(address=0, count=2, device_id=2)
        if not result.isError():
            flow_rate = result.registers[0] / 100
            tank_temp = result.registers[1] / 100
            print(f"Flow Rate: {flow_rate} L/min")
            print(f"Tank Temp: {tank_temp} °C")
        else:
            print(f"Error reading from slave 2: {result}")

        # Test writing a coil (turn on pump for slave 1)
        print("\n--- Writing to Slave 1 (Turn on pump) ---")
        result = client.write_coil(address=0, value=True, device_id=1)
        if not result.isError():
            print("✓ Pump turned ON for slave 1")
        else:
            print(f"Error writing coil: {result}")

        client.close()
        print("\n✓ Connection closed")
    else:
        print("✗ Failed to connect to Modbus server")
        print("Make sure the server is running: python plc/my_server.py")

if __name__ == "__main__":
    test_connection()

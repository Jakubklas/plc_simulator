import time
from pymodbus.client import  ModbusTcpClient
from datetime import datetime

class SCADA:
    """
    Simulates a SCADA device that constantly reads
    the PLC data over TCP Modbus
    """
    def __init__(self, host, port) -> None:
        self.host = host
        self.port = port

    def connect(self):
        print(f"Connecting to {self.host}:{self.port}...")
        self.client = ModbusTcpClient(host=self.host, port=self.port)
        if self.client.connect():
            print(f"Successfully conected.")
            return True
        else:
            print("Connection failed.")
            return False
        
    def disconnect(self):
        if self.client.close():
            print("Disconneted from the server")
            return True
        
    def read_sensors(self):
        """
        Reads all of te input registers.
        """
        for slave_id in [1, 2]:
            try:
                result = self.client.read_input_registers(
                    address=0,
                    count=10,
                    device_id=slave_id
                )

                if result.isError():
                    print(f"Failed to read input registers from device {slave_id} due to {result}")

                sensors = {
                    "flow_rate": result.registers[0] / 100.0,
                    "tank_temp": result.registers[1] / 100.0
                }

                return sensors

            except Exception as e:
                print(f"Failed to read input registers from device {slave_id} due to {e}")
                return None
    
    def read_status(self):
        for slave_id in [1, 2]:
            try:
                result = self.client.read_discrete_inputs(
                    address=0,
                    count=10,
                    device_id=slave_id
                )

                if result.isError():
                    print(f"Failed to read discrete inputs from device {slave_id} due to {result}")

                status = {
                    "pump_running": bool(result.bits[0]),
                    "tank_temp": bool(result.bits[1])
                }

                return status

            except Exception as e:
                print(f"Failed to read discrete inputs from device {slave_id} due to {e}")
                return None
    
    def read_setpoints(self):
        for slave_id in [1, 2]:
            try:
                result = self.client.read_holding_registers(
                    address=0,
                    count=10,
                    device_id=slave_id
                )

                if result.isError():
                    print(f"Failed to read setpoints from device {slave_id} due to {result}")

                status = {
                    "belt_on": bool(result.registers[0]),
                    "heater_on": bool(result.bits[1]),
                    "pump_on": bool(result.bits[2]),
                }

                return status

            except Exception as e:
                print(f"Failed to read setpoints from device {slave_id} due to {e}")
                return None
            
    def write_set_points(self, register, value):
        for slave_id in [1, 2]:
            try:
                result = self.client.write_register(
                    address=register,
                    value=value
                )
                if result.isError():
                    print(f"Failed to write setpoints to device {slave_id} due to {result}")
                    return False
                return True

            except Exception as e:
                print(f"Failed to write setpoints to device {slave_id} due to {e}")
    
    def control_pump(self, device_id, command="on"):
        return self.client.write_coil(0, (True if command=="on" else False), device_id=device_id)
    
    def control_heater(self, device_id, command="on"):
        return self.client.write_coil(1, (True if command=="on" else False), device_id=device_id)
    
    def display_dashboard(self):
        """
        Display a real-time dashboard of plant status
        """
        print("\n" + "="*70)
        print("WATER TREATMENT PLANT - LIVE DASHBOARD")
        print("="*70)
        
        sensors = self.read_sensors()
        status = self.read_status()
        setpoints = self.read_setpoints()
        
        if not sensors or not status:
            print("Error reading data from PLC")
            return
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Time: {timestamp}\n")
        
        # Sensor readings
        print("SENSOR READINGS:")
        print(f"  Flow Rate:  {sensors['flow_rate']:6.2f} m3/h")
        print(f"  Tank Temperature:  {sensors['tank_temp']:6.2f} °C")
        
        # Equipment status
        print("\nEQUIPMENT STATUS:")
        print(f"  Pump:    {'🟢 RUNNING' if status['pump_running'] else '🔴 STOPPED'}")
        print(f"  Valve:   {'🟢 RUNNING' if status['valve_open'] else '🔴 CLOSED'}")
        print(f"  Heater:  {'🟢 RUNNING' if status['heater_on'] else '🔴 OFF'}")


    def interactive_mode(self, scada):
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
        print("  q  - Quit")
        print("="*70 + "\n")
        
        while True:
            try:
                cmd = input("Command: ").strip().lower()
                
                if cmd == '1':
                    scada.control_pump(True)
                elif cmd == '0':
                    scada.control_pump(False)
                elif cmd == '3':
                    scada.control_valve(True)
                elif cmd == '2':
                    scada.control_valve(False)
                elif cmd == '5':
                    scada.control_heater(True)
                elif cmd == '4':
                    scada.control_heater(False)
                elif cmd == 'd':
                    scada.display_dashboard()
                elif cmd == 'm':
                    print("\nMonitoring mode - Press Ctrl+C to stop\n")
                    try:
                        while True:
                            scada.display_dashboard()
                            time.sleep(2)
                    except KeyboardInterrupt:
                        print("\nMonitoring stopped\n")
                elif cmd == 's':
                    try:
                        temp = float(input("Enter temperature setpoint (°C): "))
                        scaled_value = int(temp * 100)
                        if scada.write_setpoint(0, scaled_value):
                            print(f"✓ Temperature setpoint set to {temp}°C")
                    except ValueError:
                        print("Invalid temperature value")
                elif cmd == 'q':
                    break
                else:
                    print("Unknown command")
                    
            except KeyboardInterrupt:
                print("\n")
                break

        
    

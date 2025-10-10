import time
from pymodbus.client import ModbusTcpClient
from datetime import datetime


class WaterPlantSCADA:
    """
    SCADA interface for the water treatment plant PLC
    """
    
    def __init__(self, host, port=502):
        self.host = host
        self.port = port
        self.client = None


    def connect(self):
        """
        Connect to the PLC
        """
        print(f"Connecting to PLC at {self.host}:{self.port}...")
        self.client = ModbusTcpClient(self.host, port=self.port)
        
        if self.client.connect():
            print("✓ Connected successfully!\n")
            return True
        else:
            print("✗ Connection failed!")
            return False
    

    def disconnect(self):
        """
        Disconnect from the PLC
        """
        if self.client:
            self.client.close()
            print("\nDisconnected from PLC")
    

    def read_sensors(self):
        """
        Read all sensor values from input registers
        """
        try:
            # Read input registers 0-5
            result = self.client.read_input_registers(address=0, count=6, slave=0x00)
            
            if result.isError():
                print(f"Error reading sensors: {result}")
                return None
            
            # Decode values (reverse the scaling from PLC)
            sensors = {
                'temperature': result.registers[0] / 100.0,
                'inlet_pressure': result.registers[1] / 100.0,
                'outlet_pressure': result.registers[2] / 100.0,
                'flow_rate': result.registers[3] / 10.0,
                'ph_level': result.registers[4] / 100.0,
                'timestamp': result.registers[5]
            }
            
            return sensors
        except Exception as e:
            print(f"Error reading sensors: {e}")
            return None
    
    def read_status(self):
        """
        Read discrete inputs (status indicators)
        """
        try:
            result = self.client.read_discrete_inputs(address=0, count=8, slave=0x00)
            
            if result.isError():
                print(f"Error reading status: {result}")
                return None
            
            status = {
                'pump_running': result.bits[0],
                'valve_open': result.bits[1],
                'heater_on': result.bits[2],
                'flow_high': result.bits[3],
                'temp_alarm': result.bits[4],
                'pressure_alarm': result.bits[5],
                'emergency_stop': result.bits[6],
                'system_healthy': result.bits[7]
            }
            
            return status
        except Exception as e:
            print(f"Error reading status: {e}")
            return None
    
    def read_setpoints(self):
        """
        Read setpoint values from holding registers
        """
        try:
            result = self.client.read_holding_registers(address=0, count=3, slave=0x00)
            
            if result.isError():
                print(f"Error reading setpoints: {result}")
                return None
            
            setpoints = {
                'temp_setpoint': result.registers[0] / 100.0,
                'pressure_setpoint': result.registers[1] / 100.0,
                'flow_setpoint': result.registers[2] / 10.0
            }
            
            return setpoints
        except Exception as e:
            print(f"Error reading setpoints: {e}")
            return None
    
    def write_setpoint(self, register, value):
        """
        Write a setpoint to holding register
        """
        try:
            result = self.client.write_register(address=register, value=value, slave=0x00)
            
            if result.isError():
                print(f"Error writing setpoint: {result}")
                return False
            
            return True
        except Exception as e:
            print(f"Error writing setpoint: {e}")
            return False
    
    def control_pump(self, state):
        """
        Control the pump (coil address 0)
        """
        return self._write_coil(0, state, "Pump")
    
    def control_valve(self, state):
        """
        Control the valve (coil address 1)
        """
        return self._write_coil(1, state, "Valve")
    
    def control_heater(self, state):
        """
        Control the heater (coil address 2)
        """
        return self._write_coil(2, state, "Heater")
    
    def _write_coil(self, address, state, name):
        """
        Write to a coil (digital output)
        """
        try:
            result = self.client.write_coil(address=address, value=state, slave=0x00)
            
            if result.isError():
                print(f"Error controlling {name}: {result}")
                return False
            
            state_str = "ON" if state else "OFF"
            print(f"✓ {name} turned {state_str}")
            return True
        except Exception as e:
            print(f"Error controlling {name}: {e}")
            return False
    
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
        if status['pressure_alarm']:
            alarms.append("⚠️  HIGH PRESSURE")
        if status['emergency_stop']:
            alarms.append("🚨 EMERGENCY STOP ACTIVATED")
        if not status['system_healthy']:
            alarms.append("🚨 SYSTEM NOT HEALTHY")
        
        if alarms:
            for alarm in alarms:
                print(f"  {alarm}")
        else:
            print("  ✓ All systems normal")
        
        # Setpoints (if available)
        if setpoints:
            print("\nSETPOINTS:")
            print(f"  Temperature Target: {setpoints['temp_setpoint']:.1f} °C")
            print(f"  Pressure Target:    {setpoints['pressure_setpoint']:.1f} bar")
            print(f"  Flow Target:        {setpoints['flow_setpoint']:.1f} L/min")
        
        print("="*70)


def interactive_mode(scada):
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


def main():
    """
    Main program
    """
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python3 scada_client.py <PLC_IP_ADDRESS> [PORT]")
        print("Example: python3 scada_client.py 192.168.1.100")
        print("         python3 scada_client.py 192.168.1.100 5020")
        sys.exit(1)
    
    plc_ip = sys.argv[1]
    plc_port = int(sys.argv[2]) if len(sys.argv) > 2 else 502
    
    scada = WaterPlantSCADA(plc_ip, plc_port)
    
    if not scada.connect():
        sys.exit(1)
    
    try:
        # Show initial dashboard
        scada.display_dashboard()
        
        # Enter interactive mode
        interactive_mode(scada)
        
    finally:
        scada.disconnect()


if __name__ == "__main__":
    main()
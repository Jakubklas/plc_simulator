from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows, TumblingProcessingTimeWindows
from pyflink.common import Types, Time, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
import time
import socket
import threading
import sys
import os

# Add the parent directory to sys.path to import from plc folder
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from plc.scada import WaterPlantSCADA

# Execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

class PLCSocketServer:
    """
    Socket server that continuously streams PLC data
    """
    def __init__(self, host='localhost', port=9999, plc_host="localhost", plc_port=502):
        self.host = host
        self.port = port
        self.plc_host = plc_host
        self.plc_port = plc_port
        self.running = True
        self.scada = None
        self.server_socket = None
        
    def start_server(self):
        """Start socket server in background thread"""
        def server_worker():
            # Initialize SCADA connection
            self.scada = WaterPlantSCADA(self.plc_host, self.plc_port)
            
            if not self.scada.connect():
                print(f"Failed to connect to PLC at {self.plc_host}:{self.plc_port}")
                return
            
            # Create socket server
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(1)
            
            print(f"PLC Socket Server listening on {self.host}:{self.port}")
            print("Waiting for Flink to connect...")
            
            while self.running:
                try:
                    # Accept connection from Flink
                    client_socket, addr = self.server_socket.accept()
                    print(f"Flink connected from {addr}")
                    
                    # Continuously send PLC data to connected client
                    while self.running:
                        try:
                            # Read sensor data from PLC
                            sensors = self.scada.read_sensors()
                            status = self.scada.read_status()
                            
                            if sensors and status:
                                # Create timestamp in milliseconds
                                timestamp = int(time.time() * 1000)
                                
                                # Format events as newline-separated strings
                                events = [
                                    f"{timestamp}|temperature|{sensors['temperature']:.3f}|celsius",
                                    f"{timestamp}|inlet_pressure|{sensors['inlet_pressure']:.3f}|bar",
                                    f"{timestamp}|outlet_pressure|{sensors['outlet_pressure']:.3f}|bar", 
                                    f"{timestamp}|flow_rate|{sensors['flow_rate']:.3f}|lmin",
                                    f"{timestamp}|ph_level|{sensors['ph_level']:.3f}|ph",
                                    f"{timestamp}|pump_status|{1 if status['pump_running'] else 0:.3f}|boolean",
                                    f"{timestamp}|valve_status|{1 if status['valve_open'] else 0:.3f}|boolean",
                                    f"{timestamp}|heater_status|{1 if status['heater_on'] else 0:.3f}|boolean",
                                    f"{timestamp}|temp_alarm|{1 if status['temp_alarm'] else 0:.3f}|boolean",
                                    f"{timestamp}|pressure_alarm|{1 if status['pressure_alarm'] else 0:.3f}|boolean",
                                ]
                                
                                # Send all events
                                for event in events:
                                    message = event + '\n'
                                    client_socket.send(message.encode())
                            
                            time.sleep(1)  # Send data every second
                            
                        except (ConnectionResetError, BrokenPipeError):
                            print("Client disconnected")
                            break
                        except Exception as e:
                            print(f"Error reading PLC data: {e}")
                            time.sleep(2)
                    
                    client_socket.close()
                    
                except Exception as e:
                    print(f"Server error: {e}")
                    break
            
            # Cleanup
            if self.scada:
                self.scada.disconnect()
            if self.server_socket:
                self.server_socket.close()
            print("PLC Socket Server stopped")
        
        # Start server in background thread
        self.server_thread = threading.Thread(target=server_worker, daemon=True)
        self.server_thread.start()
        
    def stop(self):
        """Stop the socket server"""
        self.running = False

# Start PLC socket server
print("Starting PLC Socket Server...")
plc_server = PLCSocketServer()
plc_server.start_server()

# Give server time to start
time.sleep(2)

# Create a generator that reads from socket
def socket_data_generator():
    """Generator that continuously reads from PLC socket server"""
    while True:
        try:
            # Connect to socket server
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(("localhost", 9999))
            print("Connected to PLC socket server")
            
            # Create file object for line-by-line reading
            socket_file = client_socket.makefile('r')
            
            # Read lines continuously
            for line in socket_file:
                yield line.strip()
                
        except Exception as e:
            print(f"Socket connection error: {e}")
            time.sleep(2)  # Wait before retry
        finally:
            try:
                client_socket.close()
            except:
                pass

# Create Flink stream from socket generator
print("Creating continuous Flink stream from socket...")
stream = env.from_collection(socket_data_generator(), type_info=Types.STRING())

# Parsing function for PLC data
def parse_plc_event(line):
    """Parse PLC event string: timestamp|sensor_type|value|unit"""
    try:
        parts = line.split("|")
        if len(parts) == 4:
            timestamp = int(parts[0])
            sensor_type = parts[1]
            value = float(parts[2])
            unit = parts[3]
            return (timestamp, sensor_type, value, unit, 1)  # Add count for aggregation
        else:
            return None
    except:
        return None

# Timestamp assigner for event time processing
class PLCTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value[0]  # Use the timestamp from PLC data

# Watermark strategy
wms = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(PLCTimestampAssigner())

# Stream processing pipeline
processed_stream = stream \
    .map(lambda x: parse_plc_event(x)) \
    .filter(lambda x: x is not None) \
    .assign_timestamps_and_watermarks(wms) \
    .key_by(lambda x: x[1]) \
    .window(TumblingEventTimeWindows.of(Time.seconds(5))) \
    .reduce(lambda a, b: (
        max(a[0], b[0]),          # Latest timestamp
        a[1],                     # Sensor type (key)
        (a[2] + b[2]) / 2,        # Average value
        a[3],                     # Unit
        a[4] + b[4]               # Count of measurements
    )) \
    .map(lambda x: f"Sensor: {x[1]} | Avg: {x[2]:.2f} {x[3]} | Count: {x[4]} | Window: {time.strftime('%H:%M:%S', time.localtime(x[0]/1000))}")

# Print processed results
processed_stream.print()

# Execute continuous Flink job
print("Starting continuous Flink PLC processing job...")
print("This will run indefinitely until Ctrl+C...")

try:
    # This blocks and runs forever with continuous socket stream
    env.execute("Continuous PLC Socket Streaming")
except KeyboardInterrupt:
    print("\nUser interrupted - stopping continuous stream...")
finally:
    # Clean shutdown
    plc_server.stop()
    print("Cleanup completed")
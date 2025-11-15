import asyncio
import asyncua
from asyncua import Server
from asyncua.common.node import Node


class WaterPlantPLC:
    def __init__(self) -> None:
        self.server = Server()
        self.running = True

        # Initialize the variables to some numbers
        # Sensors
        self.temp = None
        self.presssure = None
        self.flow = None
        self.ph = None
        # States
        self.pump_on = False
        self.valve_open = False
        self.heater_on = False
        # Nodes dict (comes useful when assignong nodes in bulk)
        self.nodes = {}

    async def start_server(self):
        # Set up the server endpoint (url and name)
        await self.server.init()
        self.server.set_endpoint("opc.tcp://localhost:4840/freeopcua/server")
        self.server.set_server_name("Water Plant PLC")

        # Setup the server http namespace (uri + index)
        uri = "http://waterplant.example.com"
        idx = await self.server.register_namespace(uri)

        # Create the server tree root
        objects = self.server.get_objects_node()

        # Add objects to the node, sensor variables
        plant = await objects.add_object(idx, "WaterPlant")

        self.nodes["temp"] = plant.add_variable(idx, "Temperature", 0.0)        # Variables always floats (no scaling)
        self.nodes["pressure"] = plant.add_variable(idx, "InletPressure", 0.0)
        self.nodes["flow"] = plant.add_variable(idx, "FlowRate", 0.0)
        self.nodes["ph"] = plant.add_variable(idx, "PhRatio", 0.0)

        for k in ["temp", "pressure", "flow", "ph"]:
            await self.nodes[k].set_writeable(False)

        # Read-Write Controls
        controls = await plant.add_object(idx, "Controls")
        self.nodes["pump"] = controls.add_variable(idx, "PumpControl", False)
        self.nodes["valve"] = controls.add_variable(idx, "ValveControl", False)
        self.nodes["heater"] = controls.add_variable(idx, "HeaterControl", False)

        for k in ["pump", "valve", "heater"]:
            await self.nodes[k].set_writeable(True)

        # Read-only status
        status = await plant.add_object(idx, "Controls")
        self.nodes["pump_on"] = controls.add_variable(idx, "PumpRunning", False)
        self.nodes["valve_open"] = controls.add_variable(idx, "ValveOpen", False)
        self.nodes["heater_on"] = controls.add_variable(idx, "HeaterOn", False)

        for k in ["pump_on", "valve_open", "heater_on"]:
            await self.nodes[k].set_writeable(False)

        # Read-Write Setpoints (Holding Registers = PLC input variables)
        setpoints = await plant.add_object(idx, "SetPoints")
        self.nodes["temp_sp"] = plant.add_variable(idx, "TemperatureSetpoint", 25.0)
        self.nodes["pressure_sp"] = plant.add_variable(idx, "InletPressureSetpoint", 3.5)
        self.nodes["flow_sp"] = plant.add_variable(idx, "FlowRateSetpoint", 150.0)

        for k in ["pump_on", "valve_open", "heater_on"]:
            await self.nodes[k].set_writeable(True)
        
        await self._view_tree(uri, idx)

    async def _view_tree(self, uri, idx):
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

    async def XXX(self):
        pass

        







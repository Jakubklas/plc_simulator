from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI()

class Model(BaseModel):
    block: str
    value: float
    bit: bool


@app.get("/read_ir")
async def read_ir():
    results = client.read_input_registers(address=0, count=5, device_id=1)
    return list(r/100.0 for r in results.registers)

@app.get("/read_hr")
async def read_ir():
    results = client.read_holding_registers(address=0, count=5, device_id=1)
    return list(r/100.0 for r in results.registers)

@app.get("/read_di")
async def read_di():
    results = client.read_discrete_inputs(address=0, count=5, device_id=1)
    return list(bool(bit) for bit in results.bits)

class WriteValue(BaseModel):
    value: float

@app.post("/write_hr/{address}")
async def write_holding_register(address: int, data: WriteValue):
    scaled_value = int(data.value * 100)  # Scale to match PLC format
    result = client.write_register(address=address, value=scaled_value, device_id=1)
    if result.isError():
        return {"error": "Failed to write register"}
    return {"success": True, "address": address, "value": data.value}




if __name__ == "__main__":
    from pymodbus.client import ModbusTcpClient
    import uvicorn
    
    client = ModbusTcpClient(host="localhost", port=5020)
    try:
        client.connect()
        print("Connected to the PLC simulator.")
    except Exception as e:
        print("Connection Error:")
        print(str(e))

    uvicorn.run(app, host="localhost", port=8000)
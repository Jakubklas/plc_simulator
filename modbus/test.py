from pymodbus.client import ModbusTcpClient
import requests
import json


try:
    print("Connecting to API...")
    if True:
        for i in range(0, 5):
            response = requests.post(
                url=f"http://localhost:8000/write_hr/{i}",
                json={"value": i}
                )
    if True:
        response = requests.get(url="http://localhost:8000/read_hr")
    response.json()
except Exception as e:
    print("Request failed:")
    print(str(e))

if response.status_code == 200:
    data = response.json()
    print("Response body:")
    print()
    print(data)
else:
    print("Failed to fetch...")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.content}")
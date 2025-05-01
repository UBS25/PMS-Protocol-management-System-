import asyncio
from aiocoap.numbers.codes import GET
import random
from aio_pika import logger
from aiocoap import resource, Message, Context, Code
from fastapi import HTTPException

# Simulated sensor data storage
sensor_data = {
    "temperature": 0.0,
    "humidity": 0.0,
    "pressure": 0.0
}

# Function to generate random sensor values every few seconds
async def generate_sensor_data():
    while True:
        sensor_data["temperature"] = round(random.uniform(20, 35), 2)  # Simulated °C
        sensor_data["humidity"] = round(random.uniform(40, 80), 2)      # Simulated %
        sensor_data["pressure"] = round(random.uniform(950, 1050), 2)   # Simulated hPa

        print(f"📡 Generated Data -> Temperature: {sensor_data['temperature']}°C, "
              f"Humidity: {sensor_data['humidity']}%, Pressure: {sensor_data['pressure']} hPa")
        await asyncio.sleep(5)

class CoAPPConnectionManager:
    def __init__(self):
        pass  # Or your actual logic here

    async def read_sensor_data(self, ip: str, port: int, sensor_type: str) -> str:
        protocol = await Context.create_client_context()
        await asyncio.sleep(0.1)
        uri = f"coap://{ip}:{port}/sensor/{sensor_type}"
        logger.info(f"🔍 Requesting sensor data from: {uri}")
        try:
            request = Message(code=GET, uri=uri)
            response = await protocol.request(request).response
            return response.payload.decode('utf-8')
        except Exception as e:
            logger.error(f"❌ Failed to fetch sensor data from {uri}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

# Define CoAP resource handler
class SensorResource(resource.Resource):
    def __init__(self, sensor_type):
        super().__init__()
        self.sensor_type = sensor_type

    async def render_get(self, request):
        try:
            if self.sensor_type in sensor_data:
                response_value = sensor_data[self.sensor_type]
                print(f"📩 CoAP Response Sent -> {self.sensor_type}: {response_value}")
                return Message(payload=str(response_value).encode('utf-8'))
            else:
                error_message = "Invalid sensor parameter"
                print(f"❌ CoAP Error: {error_message}")
                return Message(code=Code.BAD_REQUEST, payload=error_message.encode('utf-8'))
        except Exception as e:
            error_message = f"Server Error: {str(e)}"
            print(f"❌ CoAP Server Error: {error_message}")
            return Message(code=Code.INTERNAL_SERVER_ERROR, payload=error_message.encode('utf-8'))

# Setup and start CoAP server
async def main():
    root = resource.Site()
    root.add_resource(('.well-known', 'core'), resource.WKCResource(root.get_resources_as_linkheader))
    
    # ✅ Register sensor-specific endpoints
    root.add_resource(('sensor', 'temperature'), SensorResource('temperature'))
    root.add_resource(('sensor', 'humidity'), SensorResource('humidity'))
    root.add_resource(('sensor', 'pressure'), SensorResource('pressure'))

    asyncio.create_task(generate_sensor_data())

    print("🚀 CoAP Server Running on 192.168.56.1...")
    await Context.create_server_context(root, bind=("192.168.56.1", 5683))

    await asyncio.sleep(1000000)

if __name__ == "__main__":
    asyncio.run(main())
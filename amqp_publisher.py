import asyncio
import aio_pika
import random
import json

async def publish_sensor_data():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()

    while True:
        data = {
            "temperature": round(random.uniform(20.0, 30.0), 2),
            "humidity": round(random.uniform(30.0, 70.0), 2),
        }
        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(data).encode()),
            routing_key="sensor_data"
        )
        print(f"📤 Published: {data}")
        await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(publish_sensor_data())

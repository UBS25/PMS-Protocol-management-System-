import asyncio
import aio_pika
import random

async def handle_message(message: aio_pika.IncomingMessage):
    async with message.process():
        print(f"📡 Received Sensor Data: {message.body.decode()}")

async def main():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()
    queue = await channel.declare_queue("sensor_data")

    print("🔄 Waiting for sensor data...")
    await queue.consume(handle_message)

    return connection

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    connection = loop.run_until_complete(main())
    loop.run_forever()

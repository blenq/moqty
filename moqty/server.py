import asyncio

from moqty.protocol import MQTTProtocol


async def main():
    loop = asyncio.get_event_loop()
    server = await loop.create_server(MQTTProtocol, port=1883)
    await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())

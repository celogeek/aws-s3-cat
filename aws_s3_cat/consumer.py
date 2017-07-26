import asyncio
import gzip

import aiobotocore


class Consumer:
    def __init__(self, queue_in, queue_out):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self._consumers = []

    async def fetch(self, consumers=8):
        async def start_consumer():
            session = aiobotocore.get_session()
            async with session.create_client('s3') as client:
                while True:
                    bucket, key = await self.queue_in.get()
                    response = await client.get_object(Bucket=bucket, Key=key)
                    raw = b""
                    async with response['Body'] as stream:
                        raw += await stream.read()
                    content = gzip.decompress(raw).decode("utf-8").split("\n")
                    for row in content:
                        if row:
                            await self.queue_out.put(row)
                    self.queue_in.task_done()

        for _ in range(consumers):
            self._consumers.append(asyncio.ensure_future(start_consumer()))

    async def display(self):
        async def start_display():
            while True:
                row = await self.queue_out.get()
                print(row)
                self.queue_out.task_done()

        self._consumers.append(asyncio.ensure_future(start_display()))

    async def done(self):
        while self._consumers:
            self._consumers.pop().cancel()
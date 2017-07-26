import asyncio
import zlib

import aiobotocore


class Consumer:
    def __init__(self, queue_in, queue_out):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self._consumers = []
        self.processed = 0

    def splitter(self, remaining, chunk, d):
        if d:
            content = d.decompress(chunk)
        else:
            content = chunk
        return (remaining + content.decode("utf-8")).split("\n")

    async def fetch(self, consumers=8):
        async def start_consumer():
            session = aiobotocore.get_session()
            async with session.create_client('s3') as client:
                while True:
                    bucket, key = await self.queue_in.get()
                    d = None
                    if key.endswith(".gz"):
                        d = zlib.decompressobj(zlib.MAX_WBITS | 32)

                    response = await client.get_object(Bucket=bucket, Key=key)
                    async with response['Body'] as stream:
                        remaining = ""
                        while True:
                            chunk = await stream.read(128 * 1024)
                            if not chunk:
                                break
                            content = self.splitter(remaining, chunk, d)
                            remaining = content.pop()

                            for row in content:
                                if row:
                                    await self.queue_out.put(row)

                        if remaining:
                            await self.queue_out.put(remaining)

                    self.queue_in.task_done()
                    self.processed += 1

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

"""streamer for aws s3 directory

Usage:
    aws-s3-cat <s3path>...

"""
import asyncio
import gzip
import re
import sys

import aiobotocore
from docopt import docopt


async def producer(queue):
    args = docopt(__doc__)

    session = aiobotocore.get_session()
    async with session.create_client('s3') as client:
        paginator = client.get_paginator('list_objects')

        for s3url in args["<s3path>"]:
            bucket, folder = re.findall("s3://([^/]+)/(.*)", s3url)[0]
            async for result in paginator.paginate(Bucket=bucket, Prefix=folder):
                for c in result.get('Contents', []):
                    await queue.put((bucket, c["Key"]))
        await queue.join()


async def consumer(queue):
    session = aiobotocore.get_session()
    async with session.create_client('s3') as client:
        while True:
            bucket, key = await queue.get()
            content = await process(client, bucket, key)
            print(content, end="")
            queue.task_done()


async def stats(queue):
    while True:
        print("Pending " + str(queue.qsize()), file=sys.stderr)
        await asyncio.sleep(1)


async def run():
    queue = asyncio.Queue()
    s = asyncio.ensure_future(stats(queue))
    consumers = [asyncio.ensure_future(consumer(queue)) for _ in range(50)]
    await producer(queue)
    for c in consumers:
        c.cancel()
    s.cancel()


async def process(client, bucket, key):
    response = await client.get_object(Bucket=bucket, Key=key)
    raw = b""
    async with response['Body'] as stream:
        raw += await stream.read()
    return gzip.decompress(raw).decode("utf-8")


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(run())

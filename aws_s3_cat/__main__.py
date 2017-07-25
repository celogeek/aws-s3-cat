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


async def go(loop):
    args = docopt(__doc__)

    session = aiobotocore.get_session(loop=loop)
    async with session.create_client('s3') as client:
        paginator = client.get_paginator('list_objects')

        tasks = []
        for s3url in args["<s3path>"]:
            bucket, folder = re.findall("s3://([^/]+)/(.*)", s3url)[0]
            async for result in paginator.paginate(Bucket=bucket, Prefix=folder):
                for c in result.get('Contents', []):
                    tasks.append(process(client, bucket, c["Key"]))

        total = len(tasks)
        done = 0
        max_tasks = 200
        print("Task created : " + str(total), file=sys.stderr)

        while tasks:
            completed, pending = await asyncio.wait(tasks[0:max_tasks], loop=loop, timeout=5)
            for t in completed:
                done += 1
                print(t.result(), end='')
            tasks = list(pending) + tasks[max_tasks:]
            print("Done : " + str(done) + " / " + str(total), file=sys.stderr)


async def process(client, bucket, key):
    response = await client.get_object(Bucket=bucket, Key=key)
    raw = b""
    async with response['Body'] as stream:
        raw += await stream.read()
    return gzip.decompress(raw).decode("utf-8")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(go(loop))

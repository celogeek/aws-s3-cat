import re

import aiobotocore


class Producer:
    def __init__(self, queue, paths):
        self.queue = queue
        self.paths = paths
        self.emited = 0

    async def list(self):
        session = aiobotocore.get_session()
        async with session.create_client('s3') as client:
            paginator = client.get_paginator('list_objects')

            for s3url in self.paths:
                bucket, folder = re.findall("s3://([^/]+)/(.*)", s3url)[0]
                async for result in paginator.paginate(Bucket=bucket, Prefix=folder):
                    for c in result.get('Contents', []):
                        await self.queue.put((bucket, c["Key"]))
                        self.emited += 1

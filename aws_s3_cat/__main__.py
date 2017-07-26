"""streamer for aws s3 directory

Usage:
    aws-s3-cat <s3path>...

"""
import asyncio

from docopt import docopt

from .consumer import Consumer
from .producer import Producer


async def run():
    args = docopt(__doc__)

    queue_in = asyncio.Queue()
    queue_out = asyncio.Queue()

    consumer = Consumer(queue_in, queue_out)

    await consumer.fetch(50)
    await consumer.display()
    await Producer(queue_in, args["<s3path>"]).list()
    await queue_in.join()
    await queue_out.join()
    await consumer.done()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(run())

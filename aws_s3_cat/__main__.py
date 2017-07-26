"""streamer for aws s3 directory

Usage:
    aws-s3-cat <s3path>...

"""
import asyncio
import sys

from docopt import docopt

from .consumer import Consumer
from .producer import Producer


def display_stats(producer, consumer):
    print("Processed {} / {}".format(consumer.processed, producer.emited), file=sys.stderr)


async def stats(producer, consumer):
    async def display_stats_async():
        while True:
            display_stats(producer, consumer)
            await asyncio.sleep(1)

    return asyncio.ensure_future(display_stats_async())


async def run():
    args = docopt(__doc__)

    queue_in = asyncio.Queue(1000)
    queue_out = asyncio.Queue(200)

    producer = Producer(queue_in, args["<s3path>"])
    consumer = Consumer(queue_in, queue_out)
    _stats = await stats(producer, consumer)

    for c in (
            consumer.fetch(50),
            consumer.display(),
            producer.list(),
            queue_in.join(),
            queue_out.join(),
            consumer.done()
    ): await c

    _stats.cancel()

    display_stats(producer, consumer)


def main():
    asyncio.get_event_loop().run_until_complete(run())


if __name__ == "__main__":
    main()

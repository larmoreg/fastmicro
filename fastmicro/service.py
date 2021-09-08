import asyncio
import atexit
from functools import partial
import logging
import signal
from typing import Any, Awaitable, Callable, List, Optional
import uvloop

from fastmicro.entrypoint import AT, BT, Entrypoint
from fastmicro.messaging import MessagingABC
from fastmicro.topic import Topic

logger = logging.getLogger(__name__)

signal.signal(signal.SIGINT, signal.SIG_DFL)
uvloop.install()


class Service:
    def __init__(
        self,
        name: str,
        messaging: MessagingABC,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()

        self.name = name
        self.messaging = messaging
        self.entrypoints: List[Entrypoint] = list()

    def entrypoint(
        self, topic: Topic[AT], reply_topic: Topic[BT], **kwargs: Any
    ) -> Callable[[Callable[[AT], Awaitable[BT]]], Entrypoint[AT, BT]]:
        def _entrypoint(callback: Callable[[AT], Awaitable[BT]]) -> Entrypoint[AT, BT]:
            entrypoint = Entrypoint(
                self.name + "_" + callback.__name__,
                self.messaging,
                callback,
                topic,
                reply_topic,
                loop=self.loop,
                **kwargs,
            )
            self.entrypoints.append(entrypoint)
            return entrypoint

        return _entrypoint

    async def start(self) -> None:
        await self.messaging.connect()

        tasks = [entrypoint.start() for entrypoint in self.entrypoints]
        await asyncio.gather(*tasks)

    def run(self) -> None:
        self.loop.run_until_complete(self.start())
        atexit.register(partial(self.loop.run_until_complete, self.stop))
        self.loop.run_forever()

    async def stop(self) -> None:
        tasks = [entrypoint.stop() for entrypoint in self.entrypoints]
        await asyncio.gather(*tasks)

        await self.messaging.cleanup()

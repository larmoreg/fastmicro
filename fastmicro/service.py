import asyncio
import atexit
from functools import partial
import logging
import signal
from typing import Awaitable, Callable, Dict, Optional
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
        self.entrypoints: Dict[str, Entrypoint] = dict()

    def entrypoint(
        self, topic: Topic[AT], reply_topic: Topic[BT]
    ) -> Callable[[Callable[[AT], Awaitable[BT]]], Entrypoint[AT, BT]]:
        def _entrypoint(callback: Callable[[AT], Awaitable[BT]]) -> Entrypoint[AT, BT]:
            name = callback.__name__
            if name in self.entrypoints:
                raise ValueError(f"Function {name} already registered in service {self.name}")

            entrypoint = Entrypoint(
                self.name + "_" + name,
                self.messaging,
                callback,
                topic,
                reply_topic,
                loop=self.loop,
            )
            self.entrypoints[name] = entrypoint
            return entrypoint

        return _entrypoint

    async def start(self) -> None:
        await self.messaging.connect()

        tasks = [entrypoint.start() for entrypoint in self.entrypoints.values()]
        await asyncio.gather(*tasks)

    def run(self) -> None:
        self.loop.run_until_complete(self.start())
        atexit.register(partial(self.loop.run_until_complete, self.stop))
        self.loop.run_forever()

    async def stop(self) -> None:
        tasks = [entrypoint.stop() for entrypoint in self.entrypoints.values()]
        await asyncio.gather(*tasks)

        await self.messaging.cleanup()

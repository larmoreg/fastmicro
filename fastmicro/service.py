import asyncio
import atexit
from functools import partial
import signal
from typing import Any, Awaitable, Callable, List, Optional
import uvloop

from fastmicro.entrypoint import AT, BT, Entrypoint
from fastmicro.messaging import MessagingABC, TopicABC

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
        self.start_callbacks: List[Callable[[], Awaitable[None]]] = list()
        self.stop_callbacks: List[Callable[[], Awaitable[None]]] = list()

    def entrypoint(
        self, topic: TopicABC[AT], reply_topic: TopicABC[BT], **kwargs: Any
    ) -> Callable[[Callable[[AT], Awaitable[BT]]], Entrypoint[AT, BT]]:
        def _entrypoint(callback: Callable[[AT], Awaitable[BT]]) -> Entrypoint[AT, BT]:
            entrypoint = Entrypoint[AT, BT](
                self.name + "_" + callback.__name__,
                callback,
                topic,
                reply_topic,
                loop=self.loop,
                **kwargs,
            )
            self.start_callbacks.append(entrypoint.start)
            self.stop_callbacks.append(entrypoint.stop)
            return entrypoint

        return _entrypoint

    async def start(self) -> None:
        await self.messaging.connect()

        tasks = [start_callback() for start_callback in self.start_callbacks]
        await asyncio.gather(*tasks)

    def run(self) -> None:
        self.loop.run_until_complete(self.start())
        atexit.register(partial(self.loop.run_until_complete, self.stop))
        self.loop.run_forever()

    async def stop(self) -> None:
        tasks = [stop_callback() for stop_callback in self.stop_callbacks]
        await asyncio.gather(*tasks)

        await self.messaging.cleanup()

import asyncio
import atexit
import logging
import signal
from typing import Any, Awaitable, Callable, Dict, List
import uvloop

from .entrypoint import AT, BT, Entrypoint
from .messaging import Messaging
from .topic import Topic

logger = logging.getLogger(__name__)

signal.signal(signal.SIGINT, signal.SIG_DFL)
uvloop.install()


class Service:
    def __init__(
        self,
        messaging: Messaging,
        name: str,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
    ) -> None:
        self.messaging = messaging
        self.name = name
        self.loop = loop
        self.entrypoints: Dict[str, Any] = dict()
        self.tasks: List[asyncio.Task[None]] = list()

    def entrypoint(
        self, topic: Topic[AT], reply_topic: Topic[BT], mock: bool = False
    ) -> Callable[[Callable[[AT], Awaitable[BT]]], Entrypoint[AT, BT]]:
        def _entrypoint(callback: Callable[[AT], Awaitable[BT]]) -> Entrypoint[AT, BT]:
            name = callback.__name__
            if name in self.entrypoints:
                raise ValueError(f"Function {name} already registered in service {self.name}")

            entrypoint = Entrypoint(self.name + "_" + name, callback, topic, reply_topic, mock=mock)
            self.entrypoints[name] = entrypoint
            return entrypoint

        return _entrypoint

    def run(self) -> None:
        for entrypoint in self.entrypoints.values():
            logger.debug(f"Starting task {entrypoint.name}")
            task = self.loop.create_task(self.process(entrypoint), name=entrypoint.name)
            self.tasks.append(task)

        atexit.register(self.kill)
        self.loop.run_forever()

    def kill(self) -> None:
        self.loop.run_until_complete(self.stop())
        self.loop.close()

    async def stop(self) -> None:
        for task in self.tasks:
            logger.debug(f"Stopping task {task.name}")
            task.cancel()
            await task

        await self.messaging.cleanup()

    @staticmethod
    async def process(entrypoint: Entrypoint[AT, BT]) -> None:
        while True:
            try:
                await entrypoint.process()
            except Exception:
                logger.exception("Processing failed")

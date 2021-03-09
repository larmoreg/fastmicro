import asyncio
import logging
import signal
from typing import Any, Awaitable, Callable, Dict, List
import uvloop

from .entrypoint import AT, BT, Entrypoint
from .topic import Topic

logger = logging.getLogger(__name__)

signal.signal(signal.SIGINT, signal.SIG_DFL)
uvloop.install()


class Service:
    def __init__(
        self,
        name: str,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
    ) -> None:
        self.name = name
        self.loop = loop
        self.entrypoints: Dict[str, Any] = dict()
        self.tasks: List[asyncio.Task[None]] = list()

    def entrypoint(
        self, topic: Topic[AT], reply_topic: Topic[BT], mock: bool = False
    ) -> Callable[[Callable[[AT], Awaitable[BT]]], Entrypoint[AT, BT]]:
        def _entrypoint(callback: Callable[[AT], Awaitable[BT]]) -> Entrypoint[AT, BT]:
            if topic.name in self.entrypoints:
                raise ValueError(f"Entrypoint already registered for topic {topic.name}")

            entrypoint = Entrypoint(self.name, callback, topic, reply_topic, mock=mock)
            self.entrypoints[topic.name] = entrypoint
            return entrypoint

        return _entrypoint

    def run(self) -> None:
        for entrypoint in self.entrypoints.values():
            task = self.loop.create_task(self.process(entrypoint), name=self.name)
            self.tasks.append(task)

        self.loop.run_forever()

    def stop(self) -> None:
        for task in self.tasks:
            task.cancel()

        self.loop.close()

    @staticmethod
    async def process(entrypoint: Entrypoint[AT, BT]) -> None:
        while True:
            try:
                await entrypoint.process()
            except Exception:
                logger.exception("Processing failed")

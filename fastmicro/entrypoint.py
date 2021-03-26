import asyncio
import logging
from typing import Awaitable, Callable, cast, Generic, Optional, TypeVar
from uuid import uuid4

import pydantic

from .messaging import Messaging
from .topic import Header, Topic

logger = logging.getLogger(__name__)

AT = TypeVar("AT", bound=pydantic.BaseModel)
BT = TypeVar("BT", bound=pydantic.BaseModel)


class Entrypoint(Generic[AT, BT]):
    def __init__(
        self,
        messaging: Messaging,
        name: str,
        callback: Callable[[AT], Awaitable[BT]],
        topic: Topic[AT],
        reply_topic: Topic[BT],
        consumer_name: str = str(uuid4()),
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
    ) -> None:
        self.messaging = messaging
        self.name = name
        self.callback = callback
        self.topic = topic
        self.reply_topic = reply_topic
        self.consumer_name = consumer_name
        self.loop = loop
        self.task: Optional[asyncio.Task[None]] = None

    async def run(self) -> None:
        if not self.task:
            logger.debug(f"Starting {self.name}")
            self.task = self.loop.create_task(self.process_loop(), name=self.name)

    async def stop(self) -> None:
        if self.task:
            logger.debug(f"Stopping {self.name}")
            self.task.cancel()
            await self.task

    async def process(self) -> None:
        # TODO: add batch processing?
        async with self.messaging.receive(
            self.topic, self.name, self.consumer_name
        ) as input_header:
            input_message = cast(AT, input_header.message)
            logger.debug(f"Processing: {input_message}")
            output_message = await self.callback(input_message)
            logger.debug(f"Result: {output_message}")
            output_header = Header(parent=input_header.uuid, message=output_message)

        await self.messaging.send(self.reply_topic, output_header)

    async def process_loop(self) -> None:
        while True:
            await self.process()

    @staticmethod
    def _is_reply(input_header: Header, output_header: Header) -> bool:
        return output_header.parent == input_header.uuid

    async def call(self, input_message: AT, mock: bool = False) -> BT:
        if mock:
            await self.messaging._prepare(self.topic.name, self.name)
        await self.messaging._prepare(self.reply_topic.name, self.name)

        logger.debug(f"Calling: {input_message}")
        input_header = await self.messaging.send(self.topic, input_message)

        if mock:
            await self.process()

        while True:
            async with self.messaging.receive(
                self.reply_topic, self.name, self.consumer_name
            ) as output_header:
                if self._is_reply(input_header, output_header):
                    output_message = output_header.message
                    break

        logger.debug(f"Result: {output_message}")
        return cast(BT, output_message)

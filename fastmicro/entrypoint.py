import asyncio
from copy import copy
import logging
from typing import Awaitable, Callable, cast, Generic, List, Optional, TypeVar
from uuid import uuid4

import pydantic

from .env import BATCH_SIZE, TIMEOUT
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

    async def start(self) -> None:
        if not self.task:
            logger.debug(f"Starting {self.name}")
            self.task = self.loop.create_task(self.process_loop(), name=self.name)

    async def stop(self) -> None:
        if self.task:
            logger.debug(f"Stopping {self.name}")
            self.task.cancel()
            await self.task

    async def process(
        self,
        mock: bool = False,
        batch_size: int = BATCH_SIZE,
        timeout: float = TIMEOUT,
    ) -> None:
        try:
            if batch_size:
                async with self.messaging.receive_batch(
                    self.topic,
                    self.name,
                    self.consumer_name,
                    batch_size=batch_size,
                    timeout=timeout,
                ) as input_headers:
                    tasks = list()
                    for input_header in input_headers:
                        input_message = cast(AT, input_header.message)
                        logger.debug(f"Processing: {input_message}")
                        tasks.append(self.callback(input_message))

                    output_messages = await asyncio.gather(*tasks)

                    output_headers = list()
                    for input_header, output_message in zip(input_headers, output_messages):
                        logger.debug(f"Result: {output_message}")
                        output_header = Header(parent=input_header.uuid, message=output_message)
                        output_headers.append(output_header)

                    await self.messaging.send_batch(self.reply_topic, output_headers)
            else:
                async with self.messaging.receive(
                    self.topic,
                    self.name,
                    self.consumer_name,
                ) as input_header:
                    input_message = cast(AT, input_header.message)
                    logger.debug(f"Processing: {input_message}")

                    output_message = await self.callback(input_message)

                    logger.debug(f"Result: {output_message}")
                    output_header = Header(parent=input_header.uuid, message=output_message)

                    await self.messaging.send(self.reply_topic, output_header)
        except Exception as e:
            logger.exception("Processing failed")
            if mock:
                raise e

    async def process_loop(self) -> None:
        while True:
            await self.process()

    @staticmethod
    def _is_reply(input_header: Header, output_header: Header) -> bool:
        return output_header.parent == input_header.uuid

    async def call(
        self,
        input_message: AT,
        mock: bool = False,
    ) -> BT:
        if mock:
            await self.messaging._subscribe(self.topic.name, self.name)
        await self.messaging._subscribe(self.reply_topic.name, self.name)

        logger.debug(f"Calling: {input_message}")
        input_header = await self.messaging.send(self.topic, input_message)

        if mock:
            try:
                await self.process(mock=mock)
            except Exception as e:
                raise e

        while True:
            async with self.messaging.receive(
                self.reply_topic, self.name, self.consumer_name
            ) as output_header:
                if self._is_reply(input_header, output_header):
                    output_message = output_header.message
                    break

        logger.debug(f"Result: {output_message}")
        return cast(BT, output_message)

    async def call_batch(
        self,
        input_messages: List[AT],
        mock: bool = False,
        batch_size: int = BATCH_SIZE,
        timeout: float = TIMEOUT,
    ) -> List[BT]:
        if mock:
            await self.messaging._subscribe(self.topic.name, self.name)
        await self.messaging._subscribe(self.reply_topic.name, self.name)

        output_messages = list()
        for i in range(0, len(input_messages), batch_size):
            j = i + batch_size
            temp_messages = input_messages[i:j]

            for input_message in temp_messages:
                logger.debug(f"Calling: {input_message}")
            input_headers = await self.messaging.send_batch(self.topic, temp_messages)

            if mock:
                try:
                    await self.process(mock=mock, batch_size=batch_size, timeout=timeout)
                except Exception as e:
                    raise e

            temp_headers = copy(input_headers)
            while temp_headers:
                async with self.messaging.receive_batch(
                    self.reply_topic,
                    self.name,
                    self.consumer_name,
                    batch_size=batch_size,
                    timeout=timeout,
                ) as output_headers:
                    for output_header in output_headers:
                        for i in range(len(temp_headers)):
                            input_header = temp_headers[i]
                            if self._is_reply(input_header, output_header):
                                output_message = output_header.message
                                output_messages.append(output_message)
                                break
                        else:
                            continue

                        del temp_headers[i]

            for output_message in output_messages:
                logger.debug(f"Result: {output_message}")

        return cast(List[BT], output_messages)

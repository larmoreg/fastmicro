import asyncio
import logging
from typing import Awaitable, Callable, Generic, List, Optional, TypeVar
from uuid import uuid4

from .env import BATCH_SIZE, TIMEOUT
from .messaging import Message, Messaging
from .topic import Topic

logger = logging.getLogger(__name__)

AT = TypeVar("AT", bound=Message)
BT = TypeVar("BT", bound=Message)


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
        self, mock: bool = False, batch_size: int = BATCH_SIZE, timeout: float = TIMEOUT
    ) -> None:
        try:
            if batch_size:
                async with self.messaging.transaction(self.reply_topic.name):
                    async with self.messaging.receive_batch(
                        self.topic,
                        self.name,
                        self.consumer_name,
                        batch_size=batch_size,
                        timeout=timeout,
                    ) as input_messages:
                        if input_messages:
                            tasks = list()
                            for input_message in input_messages:
                                logger.debug(f"Processing: {input_message}")
                                tasks.append(self.callback(input_message))

                            output_messages = await asyncio.gather(*tasks)

                            for input_message, output_message in zip(
                                input_messages, output_messages
                            ):
                                logger.debug(f"Result: {output_message}")
                                output_message.parent = input_message.uuid

                            await self.messaging.send_batch(self.reply_topic, output_messages)
            else:
                async with self.messaging.transaction(self.reply_topic.name):
                    async with self.messaging.receive(
                        self.topic, self.name, self.consumer_name
                    ) as input_message:
                        logger.debug(f"Processing: {input_message}")
                        output_message = await self.callback(input_message)

                        logger.debug(f"Result: {output_message}")
                        output_message.parent = input_message.uuid

                        await self.messaging.send(self.reply_topic, output_message)
        except Exception as e:
            logger.exception("Processing failed")
            if mock:
                raise e

    async def process_loop(self) -> None:
        while True:
            await self.process()

    async def call(self, input_message: AT, mock: bool = False) -> BT:
        if mock:
            await self.messaging._subscribe(self.topic.name, self.name)
        await self.messaging._subscribe(self.reply_topic.name, self.name)

        logger.debug(f"Calling: {input_message}")
        async with self.messaging.transaction(self.topic.name):
            await self.messaging.send(self.topic, input_message)

        if mock:
            try:
                await self.process(mock=mock)
            except Exception as e:
                raise e

        while True:
            async with self.messaging.receive(
                self.reply_topic, self.name, self.consumer_name
            ) as output_message:
                if output_message.parent == input_message.uuid:
                    break

        logger.debug(f"Result: {output_message}")
        return output_message

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

        output_messages: List[BT] = list()
        for i in range(0, len(input_messages), batch_size):
            j = i + batch_size
            temp_input_messages = input_messages[i:j]

            for input_message in temp_input_messages:
                logger.debug(f"Calling: {input_message}")
            async with self.messaging.transaction(self.topic.name):
                await self.messaging.send_batch(self.topic, temp_input_messages)

            if mock:
                try:
                    await self.process(mock=mock, batch_size=batch_size, timeout=timeout)
                except Exception as e:
                    raise e

            input_message_uuids = [input_message.uuid for input_message in temp_input_messages]
            while input_message_uuids:
                async with self.messaging.receive_batch(
                    self.reply_topic,
                    self.name,
                    self.consumer_name,
                    batch_size=batch_size,
                    timeout=timeout,
                ) as temp_output_messages:
                    if temp_output_messages:
                        for output_message in temp_output_messages:
                            for i in range(len(input_message_uuids)):
                                if output_message.parent == input_message_uuids[i]:
                                    output_messages.append(output_message)
                                    del input_message_uuids[i]
                                    break

        for output_message in output_messages:
            logger.debug(f"Result: {output_message}")

        return output_messages

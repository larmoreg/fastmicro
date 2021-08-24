import asyncio
import logging
from typing import Awaitable, Callable, cast, Generic, List, Optional, TypeVar
from uuid import uuid4

from fastmicro.env import (
    BATCH_SIZE,
    MESSAGING_TIMEOUT,
    PROCESSING_TIMEOUT,
    RESEND,
    RETRIES,
    SLEEP_TIME,
)
from fastmicro.messaging import MessageABC, MessagingABC
from fastmicro.topic import Topic

logger = logging.getLogger(__name__)

AT = TypeVar("AT", bound=MessageABC)
BT = TypeVar("BT", bound=MessageABC)


class Entrypoint(Generic[AT, BT]):
    def __init__(
        self,
        name: str,
        messaging: MessagingABC,
        callback: Callable[[AT], Awaitable[BT]],
        topic: Topic[AT],
        reply_topic: Topic[BT],
        consumer_name: str = str(uuid4()),
        broadcast: bool = False,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
    ) -> None:
        self.name = name if not broadcast else name + "_" + consumer_name
        self.messaging = messaging
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
        messaging_timeout: float = MESSAGING_TIMEOUT,
        processing_timeout: Optional[float] = PROCESSING_TIMEOUT,
    ) -> None:
        if batch_size:
            async with self.messaging.transaction(self.reply_topic.name):
                async with self.messaging.receive_batch(
                    self.topic,
                    self.name,
                    self.consumer_name,
                    batch_size=batch_size,
                    timeout=messaging_timeout,
                ) as input_messages:
                    if input_messages:
                        if logger.level >= logging.DEBUG:
                            for input_message in input_messages:
                                logger.debug(f"Processing: {input_message}")

                        attempt = 1
                        while True:
                            try:
                                tasks = [
                                    self.callback(input_message) for input_message in input_messages
                                ]
                                done, pending = await asyncio.wait(
                                    tasks,
                                    timeout=processing_timeout,
                                )
                                output_messages = [task.result() for task in done]
                                break
                            except Exception as e:
                                if mock:
                                    raise e

                                if RETRIES < 0 or attempt < RETRIES:
                                    temp = f"{attempt}"
                                    attempt += 1
                                    if RETRIES > 0:
                                        temp += f" / {RETRIES}"
                                    logger.exception(f"Processing failed; retry {temp}")
                                    if SLEEP_TIME:
                                        await asyncio.sleep(SLEEP_TIME)
                                        logger.debug(f"Sleeping for {SLEEP_TIME} sec")
                                    continue
                                else:
                                    if RESEND:
                                        logger.exception("Processing failed; resending")
                                        await self.messaging.send_batch(self.topic, input_messages)
                                    else:
                                        logger.exception("Processing failed; skipping")
                                    return

                        for input_message, output_message in zip(input_messages, output_messages):
                            output_message.parent = input_message.uuid

                        if logger.level >= logging.DEBUG:
                            for output_message in output_messages:
                                logger.debug(f"Result: {output_message}")
                        await self.messaging.send_batch(self.reply_topic, output_messages)
        else:
            async with self.messaging.transaction(self.reply_topic.name):
                async with self.messaging.receive(
                    self.topic, self.name, self.consumer_name
                ) as input_message:
                    logger.debug(f"Processing: {input_message}")

                    attempt = 1
                    while True:
                        try:
                            output_message = await asyncio.wait_for(
                                self.callback(input_message), timeout=processing_timeout
                            )
                            break
                        except Exception as e:
                            if mock:
                                raise e

                            if RETRIES < 0 or attempt < RETRIES:
                                temp = f"{attempt}"
                                attempt += 1
                                if RETRIES > 0:
                                    temp += f" / {RETRIES}"
                                logger.exception(f"Processing failed; retry {temp}")
                                if SLEEP_TIME:
                                    await asyncio.sleep(SLEEP_TIME)
                                    logger.debug(f"Sleeping for {SLEEP_TIME} sec")
                                continue
                            else:
                                if RESEND:
                                    logger.exception("Processing failed; resending")
                                    await self.messaging.send(self.topic, input_message)
                                else:
                                    logger.exception("Processing failed; skipping")
                                return

                    logger.debug(f"Result: {output_message}")
                    output_message.parent = input_message.uuid

                    await self.messaging.send(self.reply_topic, output_message)

    async def process_loop(self) -> None:
        while True:
            try:
                await self.process()
            except asyncio.exceptions.CancelledError:
                break
            except Exception:
                pass

    async def call(self, input_message: AT, mock: bool = False) -> BT:
        if mock:
            await self.messaging.subscribe(self.topic.name, self.name)
        await self.messaging.subscribe(self.reply_topic.name, self.name)

        logger.debug(f"Calling: {input_message}")
        await self.messaging.send(self.topic, input_message)

        while True:
            if mock:
                try:
                    await self.process(mock=mock)
                except Exception as e:
                    raise e

            async with self.messaging.receive(
                self.reply_topic, self.name, self.consumer_name
            ) as output_message:
                if output_message.parent == input_message.uuid:
                    break

        logger.debug(f"Result: {output_message}")
        return cast(BT, output_message)

    async def call_batch(
        self,
        input_messages: List[AT],
        mock: bool = False,
        batch_size: int = BATCH_SIZE,
        messaging_timeout: float = MESSAGING_TIMEOUT,
    ) -> List[BT]:
        if mock:
            await self.messaging.subscribe(self.topic.name, self.name)
        await self.messaging.subscribe(self.reply_topic.name, self.name)

        output_messages: List[BT] = list()
        for i in range(0, len(input_messages), batch_size):
            j = i + batch_size
            temp_input_messages = input_messages[i:j]

            if logger.level >= logging.DEBUG:
                for input_message in temp_input_messages:
                    logger.debug(f"Calling: {input_message}")
            await self.messaging.send_batch(self.topic, temp_input_messages)

            input_message_uuids = set(input_message.uuid for input_message in temp_input_messages)
            while input_message_uuids:
                if mock:
                    try:
                        await self.process(
                            mock=mock,
                            batch_size=batch_size,
                            messaging_timeout=messaging_timeout,
                        )
                    except Exception as e:
                        raise e

                async with self.messaging.receive_batch(
                    self.reply_topic,
                    self.name,
                    self.consumer_name,
                    batch_size=batch_size,
                    timeout=messaging_timeout,
                ) as temp_output_messages:
                    if temp_output_messages:
                        for output_message in temp_output_messages:
                            if output_message.parent in input_message_uuids:
                                input_message_uuids.remove(output_message.parent)
                                output_messages.append(output_message)

        if logger.level >= logging.DEBUG:
            for output_message in output_messages:
                logger.debug(f"Result: {output_message}")
        return output_messages

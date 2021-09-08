import asyncio
from copy import deepcopy
import logging
from typing import Awaitable, Callable, cast, Generic, List, Optional, TypeVar
from uuid import uuid4

from fastmicro.env import (
    BATCH_SIZE,
    MESSAGING_TIMEOUT,
    PROCESSING_TIMEOUT,
    RESENDS,
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
        self.name = name
        self.messaging = messaging
        self.callback = callback
        self.topic = topic
        self.reply_topic = reply_topic
        self.consumer_name = consumer_name
        self.broadcast = broadcast
        self.broadcast_name = name + "_" + consumer_name
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
        batch_size: int = BATCH_SIZE,
        messaging_timeout: float = MESSAGING_TIMEOUT,
        processing_timeout: Optional[float] = PROCESSING_TIMEOUT,
        retries: int = RETRIES,
        sleep_time: float = SLEEP_TIME,
        resends: int = RESENDS,
    ) -> None:
        if batch_size:
            async with self.messaging.transaction(self.reply_topic.name):
                async with self.messaging.receive_batch(
                    self.topic,
                    self.name if not self.broadcast else self.broadcast_name,
                    self.consumer_name,
                    batch_size=batch_size,
                    timeout=messaging_timeout,
                ) as input_messages:
                    if input_messages:
                        if logger.level >= logging.DEBUG:
                            for input_message in input_messages:
                                logger.debug(f"Processing: {input_message}")

                        attempt = 0
                        while True:
                            try:
                                tasks = [
                                    self.callback(input_message) for input_message in input_messages
                                ]
                                done, pending = await asyncio.wait(
                                    tasks,
                                    timeout=processing_timeout,
                                )
                                assert not pending
                                output_messages = [await task for task in done]
                            except Exception as e:
                                if retries < 0 or attempt < retries:
                                    attempt += 1
                                    temp = f"{attempt}"
                                    if retries > 0:
                                        temp += f" / {retries}"
                                    logger.exception(f"Processing failed; retry {temp}")
                                    if sleep_time:
                                        await asyncio.sleep(sleep_time)
                                        logger.debug(f"Sleeping for {sleep_time} sec")
                                    continue
                                else:
                                    if resends < 0 or input_messages[0].resends < resends:
                                        for input_message in input_messages:
                                            input_message.resends += 1
                                        temp = f"{input_messages[0].resends}"
                                        if resends > 0:
                                            temp += f" / {resends}"
                                        logger.exception(f"Processing failed; resend {temp}")
                                        await self.messaging.send_batch(self.topic, input_messages)
                                        return
                                    else:
                                        logger.exception("Processing failed; skipping")
                                        raise e
                            break

                        for input_message, output_message in zip(input_messages, output_messages):
                            output_message.parent = input_message.uuid

                        if logger.level >= logging.DEBUG:
                            for output_message in output_messages:
                                logger.debug(f"Result: {output_message}")
                        await self.messaging.send_batch(self.reply_topic, output_messages)
        else:
            async with self.messaging.transaction(self.reply_topic.name):
                async with self.messaging.receive(
                    self.topic,
                    self.name if not self.broadcast else self.broadcast_name,
                    self.consumer_name,
                ) as input_message:
                    logger.debug(f"Processing: {input_message}")

                    attempt = 0
                    while True:
                        try:
                            output_message = await asyncio.wait_for(
                                self.callback(input_message), timeout=processing_timeout
                            )
                            break
                        except Exception as e:
                            if retries < 0 or attempt < retries:
                                attempt += 1
                                temp = f"{attempt}"
                                if retries > 0:
                                    temp += f" / {retries}"
                                logger.exception(f"Processing failed; retry {temp}")
                                if sleep_time:
                                    await asyncio.sleep(sleep_time)
                                    logger.debug(f"Sleeping for {sleep_time} sec")
                                continue
                            else:
                                if resends < 0 or input_message.resends < resends:
                                    input_message.resends += 1
                                    temp = f"{input_message.resends}"
                                    if resends > 0:
                                        temp += f" / {resends}"
                                    logger.exception(f"Processing failed; resend {temp}")
                                    await self.messaging.send(self.topic, input_message)
                                    return
                                else:
                                    logger.exception("Processing failed; skipping")
                                    raise e

                    logger.debug(f"Result: {output_message}")
                    output_message.parent = input_message.uuid

                    await self.messaging.send(self.reply_topic, output_message)

    async def process_loop(self) -> None:
        while True:
            try:
                await self.process()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def call(self, input_message: AT, mock: bool = False) -> BT:
        if input_message.uuid is not None:
            input_message = deepcopy(input_message)

        if mock:
            await self.messaging.subscribe(self.topic.name, self.broadcast_name)
        await self.messaging.subscribe(self.reply_topic.name, self.broadcast_name)

        logger.debug(f"Calling: {input_message}")
        await self.messaging.send(self.topic, input_message)

        while True:
            if mock:
                try:
                    await self.process()
                except Exception as e:
                    raise e

            async with self.messaging.receive(
                self.reply_topic, self.broadcast_name, self.consumer_name
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
        input_messages = [
            deepcopy(input_message) if input_message.uuid is not None else input_message
            for input_message in input_messages
        ]

        if mock:
            await self.messaging.subscribe(self.topic.name, self.broadcast_name)
        await self.messaging.subscribe(self.reply_topic.name, self.broadcast_name)

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
                            batch_size=batch_size, messaging_timeout=messaging_timeout
                        )
                    except Exception as e:
                        raise e

                async with self.messaging.receive_batch(
                    self.reply_topic,
                    self.broadcast_name,
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

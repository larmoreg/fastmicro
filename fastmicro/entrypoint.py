import asyncio
import logging
from pydantic import BaseModel
from typing import Awaitable, Callable, cast, Generic, List, Optional, TypeVar
from uuid import uuid4

from fastmicro.env import (
    BATCH_SIZE,
    CALL_TIMEOUT,
    MESSAGING_TIMEOUT,
    PROCESSING_TIMEOUT,
    RESENDS,
    RETRIES,
    SLEEP_TIME,
)
from fastmicro.messaging import HT, MessagingABC
from fastmicro.topic import Topic

logger = logging.getLogger(__name__)

AT = TypeVar("AT", bound=BaseModel)
BT = TypeVar("BT", bound=BaseModel)


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

    async def __call__(self, input_message: AT) -> BT:
        return await self.callback(input_message)

    async def start(self) -> None:
        if not self.task:
            logger.debug(f"Starting {self.name}")
            self.task = self.loop.create_task(self.process_loop(), name=self.name)

    async def stop(self) -> None:
        if self.task:
            logger.debug(f"Stopping {self.name}")
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    async def process(
        self,
        batch_size: int = BATCH_SIZE,
        messaging_timeout: Optional[float] = MESSAGING_TIMEOUT,
        processing_timeout: Optional[float] = PROCESSING_TIMEOUT,
        retries: int = RETRIES,
        sleep_time: float = SLEEP_TIME,
        resends: int = RESENDS,
    ) -> bool:
        if batch_size:
            async with self.messaging.receive_batch(
                self.topic,
                self.name if not self.broadcast else self.broadcast_name,
                self.consumer_name,
                batch_size=batch_size,
                timeout=messaging_timeout,
            ) as (input_headers, input_messages):
                if input_headers:
                    if logger.isEnabledFor(logging.DEBUG):
                        for input_message in input_messages:
                            logger.debug(f"Processing: {input_message}")

                    attempt = 0
                    while True:
                        try:
                            output_headers = [
                                self.messaging.header_type(
                                    correlation_id=input_header.correlation_id
                                )
                                for input_header in input_headers
                            ]

                            tasks = [
                                self.callback(input_message)
                                for input_message in input_messages
                            ]
                            output_messages = await asyncio.wait_for(
                                asyncio.gather(*tasks), timeout=processing_timeout
                            )
                        except asyncio.CancelledError as e:
                            raise e
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
                                if resends < 0 or input_headers[0].resends < resends:
                                    for input_header in input_headers:
                                        input_header.resends += 1
                                    temp = f"{input_headers[0].resends}"
                                    if resends > 0:
                                        temp += f" / {resends}"
                                    logger.exception(
                                        f"Processing failed; resend {temp}"
                                    )
                                    await self.messaging.send_batch(
                                        self.topic, input_headers, input_messages
                                    )
                                    return False
                                else:
                                    logger.exception("Processing failed; skipping")
                                    for output_header in output_headers:
                                        output_header.error = str(e)
                                    await self.messaging.send_batch(
                                        self.reply_topic,
                                        output_headers,
                                    )
                                    return False
                        break

                    if logger.isEnabledFor(logging.DEBUG):
                        for output_message in output_messages:
                            logger.debug(f"Result: {output_message}")
                    await self.messaging.send_batch(
                        self.reply_topic, output_headers, output_messages
                    )
        else:
            async with self.messaging.receive(
                self.topic,
                self.name if not self.broadcast else self.broadcast_name,
                self.consumer_name,
                timeout=messaging_timeout,
            ) as (input_header, input_message):
                logger.debug(f"Processing: {input_message}")

                attempt = 0
                while True:
                    try:
                        if input_header.correlation_id:
                            output_header = self.messaging.header_type(
                                correlation_id=input_header.correlation_id
                            )

                        output_message = await asyncio.wait_for(
                            self.callback(input_message), timeout=processing_timeout
                        )
                        break
                    except asyncio.CancelledError as e:
                        raise e
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
                            if resends < 0 or input_header.resends < resends:
                                input_header.resends += 1
                                temp = f"{input_header.resends}"
                                if resends > 0:
                                    temp += f" / {resends}"
                                logger.exception(f"Processing failed; resend {temp}")
                                await self.messaging.send(
                                    self.topic, input_header, input_message
                                )
                                return False
                            else:
                                logger.exception("Processing failed; skipping")
                                output_header.error = str(e)
                                await self.messaging.send(
                                    self.reply_topic, output_header
                                )
                                return False

                logger.debug(f"Result: {output_message}")
                await self.messaging.send(
                    self.reply_topic, output_header, output_message
                )

        return True

    async def process_loop(self) -> None:
        while True:
            await self.process()

    async def call(
        self,
        input_message: AT,
        timeout: Optional[float] = CALL_TIMEOUT,
    ) -> BT:
        input_header = self.messaging.header_type(correlation_id=uuid4())

        await self.messaging.subscribe(self.reply_topic.name, self.broadcast_name)

        logger.debug(f"Calling: {input_message}")
        await self.messaging.send(self.topic, input_header, input_message)

        while True:
            async with self.messaging.receive(
                self.reply_topic,
                self.broadcast_name,
                self.consumer_name,
                timeout=timeout,
            ) as (output_header, output_message):
                if output_header.correlation_id == input_header.correlation_id:
                    break

        if output_header.error:
            raise RuntimeError(output_header.error)

        logger.debug(f"Result: {output_message}")
        return cast(BT, output_message)

    async def call_batch(
        self,
        input_messages: List[AT],
        batch_size: int = BATCH_SIZE,
        timeout: Optional[float] = CALL_TIMEOUT,
    ) -> List[BT]:
        input_headers = [
            self.messaging.header_type(correlation_id=uuid4())
            for input_message in input_messages
        ]

        await self.messaging.subscribe(self.reply_topic.name, self.broadcast_name)

        output_headers: List[HT] = list()
        output_messages: List[BT] = list()
        for i in range(0, len(input_messages), batch_size):
            j = i + batch_size
            temp_input_headers = input_headers[i:j]
            temp_input_messages = input_messages[i:j]

            if logger.isEnabledFor(logging.DEBUG):
                for input_message in temp_input_messages:
                    logger.debug(f"Calling: {input_message}")
            await self.messaging.send_batch(
                self.topic, temp_input_headers, temp_input_messages
            )

            correlation_ids = set(
                input_header.correlation_id for input_header in temp_input_headers
            )
            while correlation_ids:
                async with self.messaging.receive_batch(
                    self.reply_topic,
                    self.broadcast_name,
                    self.consumer_name,
                    batch_size=batch_size,
                    timeout=timeout,
                ) as (temp_output_headers, temp_output_messages):
                    for output_header, output_message in zip(
                        temp_output_headers, temp_output_messages
                    ):
                        if output_header.correlation_id in correlation_ids:
                            correlation_ids.remove(output_header.correlation_id)
                            output_headers.append(output_header)
                            output_messages.append(output_message)

        for output_header in output_headers:
            if output_header.error:
                raise RuntimeError(output_header.error)

        if logger.isEnabledFor(logging.DEBUG):
            for output_message in output_messages:
                logger.debug(f"Result: {output_message}")
        return output_messages

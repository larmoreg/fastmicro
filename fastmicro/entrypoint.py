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
from fastmicro.messaging import HeaderABC, TopicABC

logger = logging.getLogger(__name__)

AT = TypeVar("AT", bound=BaseModel)
BT = TypeVar("BT", bound=BaseModel)


class Entrypoint(Generic[AT, BT]):
    def __init__(
        self,
        name: str,
        callback: Callable[[AT], Awaitable[BT]],
        topic: TopicABC[AT],
        reply_topic: TopicABC[BT],
        consumer_name: str = str(uuid4()),
        broadcast: bool = False,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
    ) -> None:
        self.name = name
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
            async with self.topic.receive_batch(
                self.name if not self.broadcast else self.broadcast_name,
                self.consumer_name,
                batch_size=batch_size,
                timeout=messaging_timeout,
            ) as input_headers:
                if input_headers:
                    if logger.isEnabledFor(logging.DEBUG):
                        for input_header in input_headers:
                            logger.debug(f"Processing: {input_header.message}")

                    attempt = 0
                    while True:
                        try:
                            output_headers = [
                                self.reply_topic.header(
                                    correlation_id=input_header.correlation_id,
                                )
                                for input_header in input_headers
                            ]
                            tasks = [
                                self.callback(cast(AT, input_header.message))
                                for input_header in input_headers
                            ]
                            output_messages = await asyncio.wait_for(
                                asyncio.gather(*tasks), timeout=processing_timeout
                            )
                            for (output_header, output_message) in zip(
                                output_headers, output_messages
                            ):
                                output_header.message = output_message
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
                                    await self.topic.send_batch(input_headers)
                                    return False
                                else:
                                    logger.exception("Processing failed; skipping")
                                    for output_header in output_headers:
                                        output_header.error = str(e)
                                    await self.reply_topic.send_batch(output_headers)
                                    return False
                        break

                    if logger.isEnabledFor(logging.DEBUG):
                        for output_header in output_headers:
                            logger.debug(f"Result: {output_header.message}")
                    await self.reply_topic.send_batch(output_headers)
        else:
            async with self.topic.receive(
                self.name if not self.broadcast else self.broadcast_name,
                self.consumer_name,
                timeout=messaging_timeout,
            ) as input_header:
                logger.debug(f"Processing: {input_header.message}")

                attempt = 0
                while True:
                    try:
                        output_header = self.reply_topic.header(
                            correlation_id=input_header.correlation_id,
                        )
                        output_message = await asyncio.wait_for(
                            self.callback(cast(AT, input_header.message)),
                            timeout=processing_timeout,
                        )
                        output_header.message = output_message
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
                                await self.topic.send(input_header)
                                return False
                            else:
                                logger.exception("Processing failed; skipping")
                                output_header.error = str(e)
                                await self.reply_topic.send(output_header)
                                return False

                logger.debug(f"Result: {output_header.message}")
                await self.reply_topic.send(output_header)

        return True

    async def process_loop(self) -> None:
        while True:
            await self.process()

    async def call(
        self,
        input_message: AT,
        timeout: Optional[float] = CALL_TIMEOUT,
    ) -> BT:
        input_header = self.topic.header(correlation_id=uuid4(), message=input_message)

        await self.reply_topic.subscribe(self.broadcast_name)

        logger.debug(f"Calling: {input_header.message}")
        await self.topic.send(input_header)

        while True:
            async with self.reply_topic.receive(
                self.broadcast_name,
                self.consumer_name,
                timeout=timeout,
            ) as output_header:
                if output_header.correlation_id == input_header.correlation_id:
                    break

        if output_header.error:
            raise RuntimeError(output_header.error)

        logger.debug(f"Result: {output_header.message}")
        return cast(BT, output_header.message)

    async def call_batch(
        self,
        input_messages: List[AT],
        batch_size: int = BATCH_SIZE,
        timeout: Optional[float] = CALL_TIMEOUT,
    ) -> List[BT]:
        input_headers = [
            self.topic.header(correlation_id=uuid4(), message=input_message)
            for input_message in input_messages
        ]

        await self.reply_topic.subscribe(self.broadcast_name)

        output_headers: List[HeaderABC[BT]] = list()
        for i in range(0, len(input_headers), batch_size):
            j = i + batch_size
            temp_input_headers = input_headers[i:j]

            if logger.isEnabledFor(logging.DEBUG):
                for input_header in temp_input_headers:
                    logger.debug(f"Calling: {input_header.message}")
            await self.topic.send_batch(temp_input_headers)

            correlation_ids = set(
                input_header.correlation_id for input_header in temp_input_headers
            )
            while correlation_ids:
                async with self.reply_topic.receive_batch(
                    self.broadcast_name,
                    self.consumer_name,
                    batch_size=batch_size,
                    timeout=timeout,
                ) as temp_output_headers:
                    for output_header in temp_output_headers:
                        if output_header.correlation_id in correlation_ids:
                            correlation_ids.remove(output_header.correlation_id)
                            output_headers.append(output_header)

        for output_header in output_headers:
            if output_header.error:
                raise RuntimeError(output_header.error)

        if logger.isEnabledFor(logging.DEBUG):
            for output_header in output_headers:
                logger.debug(f"Result: {output_header.message}")
        return [cast(BT, output_header.message) for output_header in output_headers]

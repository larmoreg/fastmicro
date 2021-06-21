#!/usr/bin/env python3

import asyncio
import logging
import logging.config

import pydantic

from fastmicro.messaging.redis import RedisMessaging
from fastmicro.service import Service
from fastmicro.topic import Topic
from fastmicro.types import T

logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class User(pydantic.BaseModel):
    name: str
    delay: int = 0


class Greeting(pydantic.BaseModel):
    name: str
    greeting: str


messaging: RedisMessaging[T] = RedisMessaging()  # type: ignore
service = Service(messaging, "test")
greet_user_topic = Topic("greet_user", User)
greeting_topic = Topic("greeting", Greeting)
insult_user_topic = Topic("insult_user", User)
insult_topic = Topic("insult", Greeting)


@service.entrypoint(greet_user_topic, greeting_topic)
async def greet(user: User) -> Greeting:
    ...


@service.entrypoint(insult_user_topic, insult_topic)
async def insult(user: User) -> Greeting:
    ...


async def test() -> None:
    await messaging.connect()

    greg = User(name="Greg", delay=2)
    cara = User(name="Cara", delay=1)
    print(greg)
    print(cara)

    tasks = [insult.call(greg), greet.call(cara)]
    greetings = await asyncio.gather(*tasks)
    print(greetings)

    await messaging.cleanup()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())
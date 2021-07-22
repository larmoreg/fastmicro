#!/usr/bin/env python3

import asyncio
import logging
import logging.config

from fastmicro.messaging.kafka import Message, Messaging
from fastmicro.service import Service
from fastmicro.topic import Topic

logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


messaging: Messaging = Messaging()
service = Service("test", messaging)


class User(Message):
    name: str
    delay: int = 0


class Greeting(Message):
    name: str
    greeting: str


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

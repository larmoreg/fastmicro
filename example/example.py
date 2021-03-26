#!/usr/bin/env python3

import asyncio
import logging
import logging.config

import pydantic

from fastmicro.messaging import RedisMessaging
from fastmicro.service import Service
from fastmicro.topic import Topic

logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class User(pydantic.BaseModel):
    name: str
    delay: int = 0


class Greeting(pydantic.BaseModel):
    name: str
    greeting: str


messaging = RedisMessaging()
service = Service(messaging, "test")
greet_user_topic = Topic("greet_user", User)
greeting_topic = Topic("greeting", Greeting)
insult_user_topic = Topic("insult_user", User)
insult_topic = Topic("insult", Greeting)


@service.entrypoint(greet_user_topic, greeting_topic)
async def greet(user: User) -> Greeting:
    await asyncio.sleep(user.delay)
    greeting = Greeting(name=user.name, greeting=f"Hello, {user.name}!")
    return greeting


@service.entrypoint(insult_user_topic, insult_topic)
async def insult(user: User) -> Greeting:
    await asyncio.sleep(user.delay)
    greeting = Greeting(name=user.name, greeting=f"You smell, {user.name}!")
    return greeting


if __name__ == "__main__":
    service.run()

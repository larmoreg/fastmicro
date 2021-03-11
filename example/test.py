#!/usr/bin/env python3

import asyncio
import logging

import pydantic

from fastmicro.messaging import RedisMessaging
from fastmicro.service import Service
from fastmicro.topic import Topic

logger = logging.getLogger(__name__)


class User(pydantic.BaseModel):
    name: str
    delay: int = 0


class Greeting(pydantic.BaseModel):
    name: str
    greeting: str


service = Service("test")
messaging = RedisMessaging()
user_topic = Topic(messaging, "user", User)
greeting_topic = Topic(messaging, "greeting", Greeting)


@service.entrypoint(user_topic, greeting_topic)
async def greet(user: User) -> Greeting:
    await asyncio.sleep(user.delay)
    return Greeting(name=user.name, greeting=f"Hello, {user.name}!")


if __name__ == "__main__":
    user = User(name="Greg")
    logger.error(user)
    loop = asyncio.get_event_loop()
    greeting = loop.run_until_complete(greet.call(user))
    logger.error(greeting)

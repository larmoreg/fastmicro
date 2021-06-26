#!/usr/bin/env python3

import asyncio
import logging
import logging.config

from fastmicro.messaging.kafka import KafkaMessage, KafkaMessaging
from fastmicro.service import Service
from fastmicro.topic import Topic

logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


messaging: KafkaMessaging = KafkaMessaging()
service = Service(messaging, "test")


class User(KafkaMessage):
    name: str
    delay: int = 0


class Greeting(KafkaMessage):
    name: str
    greeting: str


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

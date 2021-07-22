#!/usr/bin/env python3

from fastmicro.messaging.redis import Message, Messaging
from fastmicro.service import Service
from fastmicro.topic import Topic

messaging: Messaging = Messaging()
service = Service("test", messaging)


class User(Message):
    name: str


class Greeting(Message):
    name: str
    greeting: str


greet_user_topic = Topic("greet_user", User)
greeting_topic = Topic("greeting", Greeting)


@service.entrypoint(greet_user_topic, greeting_topic)
async def greet(user: User) -> Greeting:
    print(user.dict())
    greeting = Greeting(name=user.name, greeting=f"Hello, {user.name}!")
    print(greeting.dict())
    return greeting


if __name__ == "__main__":
    service.run()

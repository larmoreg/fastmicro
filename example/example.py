#!/usr/bin/env python3

from pydantic import BaseModel

from fastmicro.messaging.redis import Messaging
from fastmicro.service import Service
from fastmicro.topic import Topic


class User(BaseModel):
    name: str


class Greeting(BaseModel):
    name: str
    greeting: str


messaging: Messaging = Messaging()
service = Service("test", messaging)

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

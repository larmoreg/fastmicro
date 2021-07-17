import asyncio
import logging
from nats.aio.client import Client as NATS
from pydantic import Field
from stan.aio.client import Client as STAN, Msg
import stan.pb.protocol_pb2 as protocol
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from fastmicro.env import (
    NATS_SERVERS,
    NATS_CLUSTER_ID,
    NATS_CLIENT_ID,
)
from fastmicro.messaging import MessageABC, MessagingABC
from fastmicro.topic import Topic

logger = logging.getLogger(__name__)


class Message(MessageABC):
    sequence: Optional[int] = Field(None, hidden=True)
    subject: Optional[str] = Field(None, hidden=True)
    ack_inbox: Optional[str] = Field(None, hidden=True)


T = TypeVar("T", bound=Message)


def async_partial(f: Callable[..., Any], *args: Any) -> Callable[..., Awaitable[Any]]:
    async def f2(*args2: Any) -> Any:
        result = f(*args, *args2)
        if asyncio.iscoroutinefunction(f):
            result = await result
        return result

    return f2


class Messaging(MessagingABC):
    def __init__(
        self,
        servers: str = NATS_SERVERS,
        cluster_id: str = NATS_CLUSTER_ID,
        client_id: str = NATS_CLIENT_ID,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()
        super().__init__(self.loop)

        self.servers = servers.split(",")
        self.cluster_id = cluster_id
        self.client_id = client_id
        self.nc: Any = None
        self.scs: Dict[str, Any] = dict()
        self.subs: Dict[str, Any] = dict()
        self.queues: Dict[Tuple[str, str], asyncio.Queue[Msg]] = dict()

    async def connect(self) -> None:
        self.nc = NATS()
        await self.nc.connect(self.servers, loop=self.loop)

        self.sc = STAN()
        await self.sc.connect(self.cluster_id, self.client_id, nats=self.nc)

    async def cleanup(self) -> None:
        for sub in self.subs.values():
            await sub.unsubscribe()

        assert self.sc
        await self.sc.close()

        assert self.nc
        await self.nc.close()

    async def _get_queue(self, topic_name: str, group_name: str) -> asyncio.Queue[Msg]:
        key = (topic_name, group_name)
        if key not in self.queues:
            self.queues[key] = asyncio.Queue(loop=self.loop)
        return self.queues[key]

    async def subscribe(self, topic_name: str, group_name: str) -> None:
        async def _cb(queue: asyncio.Queue[Msg], msg: Msg) -> None:
            await queue.put(msg)

        if topic_name not in self.subs:
            queue = await self._get_queue(topic_name, group_name)
            sub = await self.sc.subscribe(
                topic_name,
                queue=group_name,
                durable_name="durable",
                cb=async_partial(_cb, queue),
                manual_acks=True,
                ack_wait=60,  # FIXME: what to do about this...?
            )
            self.subs[topic_name] = sub

    async def _receive(self, topic: Topic[T], group_name: str, consumer_name: str) -> T:
        queue: asyncio.Queue[Msg] = await self._get_queue(topic.name, group_name)
        msg = await queue.get()

        message = await topic.deserialize(msg.data)
        message.sequence = msg.proto.sequence
        message.subject = msg.proto.subject
        message.ack_inbox = msg.sub.ack_inbox
        return message

    async def _ack(self, topic_name: str, group_name: str, message: T) -> None:
        ack_proto = protocol.Ack()
        ack_proto.subject = message.subject
        ack_proto.sequence = message.sequence
        await self.nc.publish(message.ack_inbox, ack_proto.SerializeToString())

    async def _nack(self, topic_name: str, group_name: str, message: T) -> None:
        pass

    async def _nack_batch(self, topic_name: str, group_name: str, messages: List[T]) -> None:
        pass

    async def _send(self, topic: Topic[T], message: T) -> None:
        serialized = await topic.serialize(message)
        await self.sc.publish(topic.name, serialized)

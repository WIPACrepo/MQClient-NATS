"""Back-end using NATS."""


import logging
import math
import time
from functools import partial
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    List,
    Optional,
    TypeVar,
    cast,
)

from mqclient import backend_interface, log_msgs
from mqclient.backend_interface import (
    RETRY_DELAY,
    TIMEOUT_MILLIS_DEFAULT,
    TRY_ATTEMPTS,
    ClosingFailedExcpetion,
    Message,
    Pub,
    RawQueue,
    Sub,
)

import nats  # type: ignore[import]

T = TypeVar("T")  # the callable/awaitable return type


async def _anext(gen: AsyncGenerator[Any, Any], default: Any) -> Any:
    """Provide the functionality of python 3.10's `anext()`.

    https://docs.python.org/3/library/functions.html#anext
    """
    try:
        return await gen.__anext__()
    except StopAsyncIteration:
        return default


async def try_call(self: "NATS", func: Callable[..., Awaitable[T]]) -> T:
    """Call `func` with auto-retries."""
    i = 0
    while True:
        if i > 0:
            logging.debug(
                f"{log_msgs.TRYCALL_CONNECTION_ERROR_TRY_AGAIN} (attempt #{i+1})..."
            )

        try:
            return await func()
        except nats.errors.TimeoutError:
            raise
        except Exception as e:  # pylint:disable=broad-except
            logging.debug(f"[try_call()] Encountered exception: '{e}'")
            if i == TRY_ATTEMPTS - 1:
                logging.debug(log_msgs.TRYCALL_CONNECTION_ERROR_MAX_RETRIES)
                raise

        await self.close()
        time.sleep(RETRY_DELAY)
        await self.connect()
        i += 1


class NATS(RawQueue):
    """Base NATS wrapper, using JetStream.

    Extends:
        RawQueue
    """

    def __init__(self, endpoint: str, stream_id: str, subject: str) -> None:
        super().__init__()
        self.endpoint = endpoint
        self.subject = subject
        self.stream_id = stream_id

        self._nats_client: Optional[nats.aio.client.Client] = None
        self.js: Optional[nats.js.JetStream] = None

        logging.debug(f"Stream & Subject: {stream_id}/{self.subject}")

    async def connect(self) -> None:
        """Set up connection and channel."""
        await super().connect()
        self._nats_client = cast(
            nats.aio.client.Client, await nats.connect(self.endpoint)
        )
        # Create JetStream context
        self.js = cast(
            nats.js.JetStream,
            self._nats_client.jetstream(timeout=TIMEOUT_MILLIS_DEFAULT // 1000),
        )
        await self.js.add_stream(name=self.stream_id, subjects=[self.subject])

    async def close(self) -> None:
        """Close connection."""
        await super().close()
        if not self._nats_client:
            raise ClosingFailedExcpetion("No connection to close.")
        await self._nats_client.flush()
        await self._nats_client.close()


class NATSPub(NATS, Pub):
    """Wrapper around PublisherClient, using JetStream.

    Extends:
        NATS
        Pub
    """

    def __init__(self, endpoint: str, stream_id: str, subject: str):
        logging.debug(f"{log_msgs.INIT_PUB} ({endpoint}; {stream_id}; {subject})")
        super().__init__(endpoint, stream_id, subject)
        # NATS is pub-centric, so no extra instance needed

    async def connect(self) -> None:
        """Set up pub, then create topic and any subscriptions indicated."""
        logging.debug(log_msgs.CONNECTING_PUB)
        await super().connect()
        logging.debug(log_msgs.CONNECTED_PUB)

    async def close(self) -> None:
        """Close pub (no-op)."""
        logging.debug(log_msgs.CLOSING_PUB)
        await super().close()
        logging.debug(log_msgs.CLOSED_PUB)

    async def send_message(self, msg: bytes) -> None:
        """Send a message (publish)."""
        logging.debug(log_msgs.SENDING_MESSAGE)
        if not self.js:
            raise RuntimeError("JetStream is not connected")

        ack: nats.js.api.PubAck = await try_call(
            self, partial(self.js.publish, self.subject, msg)
        )
        logging.debug(f"Sent Message w/ Ack: {ack}")
        logging.debug(log_msgs.SENT_MESSAGE)


class NATSSub(NATS, Sub):
    """Wrapper around queue with prefetch-queue, using JetStream.

    Extends:
        NATS
        Sub
    """

    def __init__(self, endpoint: str, stream_id: str, subject: str):
        logging.debug(f"{log_msgs.INIT_SUB} ({endpoint}; {stream_id}; {subject})")
        super().__init__(endpoint, stream_id, subject)
        self._subscription: Optional[nats.js.JetStream.PullSubscription] = None
        self.prefetch = 1

    async def connect(self) -> None:
        """Set up sub (pull subscription)."""
        logging.debug(log_msgs.CONNECTING_SUB)
        await super().connect()
        if not self.js:
            raise RuntimeError("JetStream is not connected.")

        self._subscription = cast(
            nats.js.JetStream.PullSubscription,
            await self.js.pull_subscribe(self.subject, "psub"),
        )
        logging.debug(log_msgs.CONNECTED_SUB)

    async def close(self) -> None:
        """Close sub."""
        logging.debug(log_msgs.CLOSING_SUB)
        if not self._subscription:
            raise ClosingFailedExcpetion("No sub to close.")
        await self._subscription._sub.drain()  # pylint:disable=protected-access
        await super().close()
        logging.debug(log_msgs.CLOSED_SUB)

    @staticmethod
    def _to_message(  # type: ignore[override]  # noqa: F821 # pylint: disable=W0221
        msg: nats.aio.msg.Msg,  # pylint: disable=no-member
    ) -> Optional[Message]:
        """Transform NATS-Message to Message type."""
        return Message(cast(str, msg.reply), cast(bytes, msg.data))

    def _from_message(self, msg: Message) -> nats.aio.msg.Msg:
        """Transform Message instance to NATS-Message.

        Assumes the message came from this NATSSub instance.
        """
        return nats.aio.msg.Msg(
            subject=self.subject,
            reply=msg.msg_id,
            data=msg.data,
            sid=0,  # default
            client=self._nats_client,
            headers=None,  # default
        )

    async def _get_messages(
        self, timeout_millis: Optional[int], num_messages: int
    ) -> List[Message]:
        """Get n messages.

        The subscriber pulls a specific number of messages. The actual
        number of messages pulled may be smaller than `num_messages`.
        """
        if not self._subscription:
            raise RuntimeError("Subscriber is not connected")

        if not timeout_millis:
            timeout_millis = TIMEOUT_MILLIS_DEFAULT

        try:
            nats_msgs: List[nats.aio.msg.Msg] = await try_call(
                self,
                partial(
                    self._subscription.fetch,
                    num_messages,
                    int(math.ceil(timeout_millis / 1000)),
                ),
            )
        except nats.errors.TimeoutError:
            return []

        msgs = []
        for recvd in nats_msgs:
            msg = self._to_message(recvd)
            if msg:
                msgs.append(msg)
        return msgs

    async def get_message(
        self, timeout_millis: Optional[int] = TIMEOUT_MILLIS_DEFAULT
    ) -> Optional[Message]:
        """Get a message."""
        logging.debug(log_msgs.GETMSG_RECEIVE_MESSAGE)
        if not self._subscription:
            raise RuntimeError("Subscriber is not connected.")

        try:
            msg = (await self._get_messages(timeout_millis, 1))[0]
            logging.debug(f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({msg}).")
            return msg
        except IndexError:
            logging.debug(log_msgs.GETMSG_NO_MESSAGE)
            return None

    async def _gen_messages(
        self, timeout_millis: Optional[int], num_messages: int
    ) -> AsyncGenerator[Message, None]:
        """Continuously generate messages until there are no more."""
        if not self._subscription:
            raise RuntimeError("Subscriber is not connected.")

        while True:
            msgs = await self._get_messages(timeout_millis, num_messages)
            if not msgs:
                return
            for msg in msgs:
                yield msg

    async def ack_message(self, msg: Message) -> None:
        """Ack a message from the queue."""
        logging.debug(log_msgs.ACKING_MESSAGE)
        if not self._subscription:
            raise RuntimeError("subscriber is not connected")

        # Acknowledges the received messages so they will not be sent again.
        await try_call(self, partial(self._from_message(msg).ack))
        logging.debug(f"{log_msgs.ACKED_MESSAGE} ({msg.msg_id!r}).")

    async def reject_message(self, msg: Message) -> None:
        """Reject (nack) a message from the queue."""
        logging.debug(log_msgs.NACKING_MESSAGE)
        if not self._subscription:
            raise RuntimeError("subscriber is not connected")

        await try_call(self, partial(self._from_message(msg).nak))  # yes, it's "nak"
        logging.debug(f"{log_msgs.NACKED_MESSAGE} ({msg.msg_id!r}).")

    async def message_generator(
        self, timeout: int = 60, propagate_error: bool = True
    ) -> AsyncGenerator[Optional[Message], None]:
        """Yield Messages.

        Generate messages with variable timeout.
        Yield `None` on `throw()`.

        Keyword Arguments:
            timeout {int} -- timeout in seconds for inactivity (default: {60})
            propagate_error {bool} -- should errors from downstream code kill the generator? (default: {True})
        """
        logging.debug(log_msgs.MSGGEN_ENTERED)
        if not self._subscription:
            raise RuntimeError("subscriber is not connected")

        msg = None
        try:
            gen = self._gen_messages(timeout * 1000, self.prefetch)
            while True:
                # get message
                logging.debug(log_msgs.MSGGEN_GET_NEW_MESSAGE)
                msg = await _anext(gen, None)
                if msg is None:
                    logging.info(log_msgs.MSGGEN_NO_MESSAGE_LOOK_BACK_IN_QUEUE)
                    break

                # yield message to consumer
                try:
                    logging.debug(f"{log_msgs.MSGGEN_YIELDING_MESSAGE} [{msg}]")
                    yield msg
                # consumer throws Exception...
                except Exception as e:  # pylint: disable=W0703
                    logging.debug(log_msgs.MSGGEN_DOWNSTREAM_ERROR)
                    if propagate_error:
                        logging.debug(log_msgs.MSGGEN_PROPAGATING_ERROR)
                        raise
                    logging.warning(
                        f"{log_msgs.MSGGEN_EXCEPTED_DOWNSTREAM_ERROR} {e}.",
                        exc_info=True,
                    )
                    yield None  # hand back to consumer
                # consumer requests again, aka next()
                else:
                    pass

        # generator exit (explicit close(), or break in consumer's loop)
        except GeneratorExit:
            logging.debug(log_msgs.MSGGEN_GENERATOR_EXITING)
            logging.debug(log_msgs.MSGGEN_GENERATOR_EXITED)


class Backend(backend_interface.Backend):
    """NATS Pub-Sub Backend Factory.

    Extends:
        Backend
    """

    @staticmethod
    async def create_pub_queue(
        address: str, name: str, auth_token: str = ""
    ) -> NATSPub:
        """Create a publishing queue.

        # NOTE - `auth_token` is not used currently
        """
        q = NATSPub(  # pylint: disable=invalid-name
            address, name + "-stream", name + "-subject"
        )
        await q.connect()
        return q

    @staticmethod
    async def create_sub_queue(
        address: str, name: str, prefetch: int = 1, auth_token: str = ""
    ) -> NATSSub:
        """Create a subscription queue.

        # NOTE - `auth_token` is not used currently
        """
        q = NATSSub(  # pylint: disable=invalid-name
            address, name + "-stream", name + "-subject"
        )
        q.prefetch = prefetch
        await q.connect()
        return q

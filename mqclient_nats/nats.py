"""Back-end using NATS."""

import logging
import os
from typing import Generator, List, Optional, Tuple, cast

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

        self._connection: Optional[nats.aio.client.Client] = None
        self.js: Optional[nats.js.JetStream] = None

        logging.debug(f"Stream & Subject: {stream_id}/{self.subject}")

    def connect(self) -> None:
        """Set up connection and channel."""
        super().connect()
        self._connection = cast(  # type: ignore[redundant-cast]
            nats.aio.client.Client, await nats.connect(self.endpoint)
        )
        # Create JetStream context
        self.js = cast(  # type: ignore[redundant-cast]
            nats.js.JetStream,
            self._connection.jetstream(timeout=TIMEOUT_MILLIS_DEFAULT * 1000),
        )
        await self.js.add_stream(name=self.stream_id, subjects=[self.subject])

    def close(self) -> None:
        """Close connection."""
        super().close()
        if not self._connection:
            raise ClosingFailedExcpetion("No connection to close.")
        self._connection.close()


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

    def connect(self) -> None:
        """Set up pub, then create topic and any subscriptions indicated."""
        logging.debug(log_msgs.CONNECTING_PUB)
        super().connect()

    def close(self) -> None:
        """Close pub (no-op)."""
        logging.debug(log_msgs.CLOSING_PUB)
        super().close()
        logging.debug(log_msgs.CLOSED_PUB)

    def send_message(self, msg: bytes) -> None:
        """Send a message (publish)."""
        logging.debug(log_msgs.SENDING_MESSAGE)
        if not self.js:
            raise RuntimeError("JetStream is not connected")

        ack: nats.js.api.PubAck = await self.js.publish(self.subject, msg)
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
        self.sub: Optional[nats.js.JetStream.PullSubscription] = None
        self.prefetch = 1

    def connect(self) -> None:
        """Set up sub (pull subscription)."""
        logging.debug(log_msgs.CONNECTING_SUB)
        super().connect()
        if not self.js:
            raise RuntimeError("JetStream is not connected.")

        # TODO - configure: prefetch etc.
        self.sub = cast(  # type: ignore[redundant-cast]
            nats.js.JetStream.PullSubscription,
            await self.js.pull_subscribe(self.subject, "psub"),
        )
        logging.debug(log_msgs.CONNECTED_SUB)

    def close(self) -> None:
        """Close sub."""
        logging.debug(log_msgs.CLOSING_SUB)
        super().close()
        if not self.sub:
            raise ClosingFailedExcpetion("No sub to close.")
        logging.debug(log_msgs.CLOSED_SUB)

    def get_message(
        self, timeout_millis: Optional[int] = TIMEOUT_MILLIS_DEFAULT
    ) -> Optional[Message]:
        """Get a message."""
        logging.debug(log_msgs.GETMSG_RECEIVE_MESSAGE)
        if not self.sub:
            raise RuntimeError("Subscriber is not connected.")

        if not timeout_millis:
            timeout_millis = TIMEOUT_MILLIS_DEFAULT

        try:
            msg: nats.aio.msg.Msg = await self.sub.fetch(1, timeout_millis * 1000)[0]
            logging.debug(f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({msg}).")
            # TODO - convert message type
            return msg
        except IndexError:  # TODO - is this needed?
            logging.debug(log_msgs.GETMSG_NO_MESSAGE)
            return None

    def _gen_messages(
        self, timeout_millis: Optional[int], num_messages: int
    ) -> Generator[Message, None, None]:
        """Continuously generate messages until there are no more."""
        if not self.sub:
            raise RuntimeError("Subscriber is not connected.")

        if not timeout_millis:
            timeout_millis = TIMEOUT_MILLIS_DEFAULT

        while True:
            msgs: List[nats.aio.msg.Msg] = await self.sub.fetch(
                num_messages, timeout_millis * 1000
            )
            if not msgs:
                return
            for msg in msgs:
                yield msg

    def ack_message(self, msg: Message) -> None:
        """Ack a message from the queue."""
        logging.debug(log_msgs.ACKING_MESSAGE)
        if not self.sub:
            raise RuntimeError("subscriber is not connected")

        # Acknowledges the received messages so they will not be sent again.
        await msg.ack()  # TODO - convert back to nat's Msg type
        logging.debug(f"{log_msgs.ACKED_MESSAGE} ({msg.msg_id!r}).")

    def reject_message(self, msg: Message) -> None:
        """Reject (nack) a message from the queue."""
        logging.debug(log_msgs.NACKING_MESSAGE)
        if not self.sub:
            raise RuntimeError("subscriber is not connected")

        # TODO - convert back to nat's Msg type
        await msg.nak()  # yes, it's "nak"
        logging.debug(f"{log_msgs.NACKED_MESSAGE} ({msg.msg_id!r}).")

    def message_generator(
        self, timeout: int = 60, propagate_error: bool = True
    ) -> Generator[Optional[Message], None, None]:
        """Yield Messages.

        Generate messages with variable timeout.
        Yield `None` on `throw()`.

        Keyword Arguments:
            timeout {int} -- timeout in seconds for inactivity (default: {60})
            propagate_error {bool} -- should errors from downstream code kill the generator? (default: {True})
        """
        logging.debug(log_msgs.MSGGEN_ENTERED)
        if not self.sub:
            raise RuntimeError("subscriber is not connected")

        msg = None
        try:
            gen = self._gen_messages(timeout * 1000, self.prefetch)
            while True:
                # get message
                logging.debug(log_msgs.MSGGEN_GET_NEW_MESSAGE)
                msg = next(gen, None)
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

    SUBJECT_ID = "i3-nats-subject"

    @staticmethod
    def create_pub_queue(address: str, name: str, auth_token: str = "") -> NATSPub:
        """Create a publishing queue.

        # NOTE - `auth_token` is not used currently
        """
        q = NATSPub(address, name, Backend.SUBJECT_ID)  # pylint: disable=invalid-name
        q.connect()
        return q

    @staticmethod
    def create_sub_queue(
        address: str, name: str, prefetch: int = 1, auth_token: str = ""
    ) -> NATSSub:
        """Create a subscription queue.

        # NOTE - `auth_token` is not used currently
        """
        q = NATSSub(address, name, Backend.SUBJECT_ID)  # pylint: disable=invalid-name
        q.prefetch = prefetch
        q.connect()
        return q

"""Back-end using GCP."""

import logging
import os
from typing import Generator, List, Optional, Tuple

from google.api_core import exceptions, retry  # type: ignore[import]
from google.cloud import pubsub  # type: ignore[import]
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

_DEFAULT_RETRY = retry.Retry(
    initial=RETRY_DELAY,
    # maximum=RETRY_DELAY,  # same as initial, not really needed if multiplier=1.0
    multiplier=1.0,  # change if we want exponential retries
    deadline=RETRY_DELAY * (TRY_ATTEMPTS - 1),
)  # Ex: RETRY_DELAY=1, TRY_ATTEMPTS=3: <try> ...1sec... <try> ...1sec... <try>


class GCP(RawQueue):
    """Base GCP wrapper.

    Extends:
        RawQueue
    """

    def __init__(self, endpoint: str, project_id: str, topic_id: str) -> None:
        super().__init__()
        self.endpoint = endpoint
        self._project_id = project_id

        # create a temporary PublisherClient just to get `topic_path`
        self._topic_path = (
            pubsub.PublisherClient().topic_path(  # pylint: disable=no-member
                self._project_id, topic_id
            )
        )
        logging.debug(f"Topic Path: {self._topic_path}")

    def connect(self) -> None:
        """Set up connection and channel."""
        super().connect()

    def close(self) -> None:
        """Close connection."""
        super().close()

    @staticmethod
    def _create_and_connect_sub(
        endpoint: str, project_id: str, topic_path: str, subscription_id: str
    ) -> Tuple[pubsub.SubscriberClient, str]:
        """Create a subscription, then return a subscriber instance and path.

        If the subscription already exists, the subscription is unaffected.
        """
        sub = pubsub.SubscriberClient(client_options={"api_endpoint": endpoint})
        subscription_path = sub.subscription_path(  # pylint: disable=no-member
            project_id, subscription_id
        )

        try:
            sub.create_subscription(  # pylint: disable=no-member
                request={
                    "name": subscription_path,
                    "topic": topic_path,
                    "ack_deadline_seconds": 600,  # 10min is the GCP max
                },
                retry=_DEFAULT_RETRY,
            )
            logging.debug(f"Subscription created ({subscription_path})")
        except exceptions.AlreadyExists:
            logging.debug(f"Subscription already exists ({subscription_path})")

        return sub, subscription_path


class GCPPub(GCP, Pub):
    """Wrapper around PublisherClient, with topic and subscription creation.

    Extends:
        GCP
        Pub
    """

    def __init__(
        self,
        endpoint: str,
        project_id: str,
        topic_id: str,
        subscription_ids: Optional[List[str]] = None,
    ):
        logging.debug(
            f"{log_msgs.INIT_PUB} "
            f"({endpoint}; {project_id}; {topic_id}; {subscription_ids})"
        )
        super().__init__(endpoint, project_id, topic_id)
        self.pub: Optional[pubsub.PublisherClient] = None
        self.subscription_ids = subscription_ids if subscription_ids else []

    def connect(self) -> None:
        """Set up pub, then create topic and any subscriptions indicated."""
        logging.debug(log_msgs.CONNECTING_PUB)
        super().connect()

        self.pub = pubsub.PublisherClient(
            publisher_options=pubsub.types.PublisherOptions(
                enable_message_ordering=True
            ),
            client_options={"api_endpoint": self.endpoint},
        )

        try:
            self.pub.create_topic(  # pylint: disable=no-member
                request={"name": self._topic_path}, retry=_DEFAULT_RETRY
            )
            logging.debug(f"Topic created ({self._topic_path})")
        except exceptions.AlreadyExists:
            logging.debug(f"Topic already exists ({self._topic_path})")
        finally:
            logging.debug(log_msgs.CONNECTED_PUB)

        # Create Any Subscriptions
        # NOTE - A message published before a given subscription was created will
        #  usually not be delivered for that subscription. Thus, a message published
        #  to a topic that has no subscription will not be delivered to any subscriber.
        for sub_id in self.subscription_ids:
            GCP._create_and_connect_sub(
                self.endpoint, self._project_id, self._topic_path, sub_id
            )

    def close(self) -> None:
        """Close pub (no-op)."""
        logging.debug(log_msgs.CLOSING_PUB)
        super().close()
        if not self.pub:
            raise ClosingFailedExcpetion("No pub to sub.")
        logging.debug(log_msgs.CLOSED_PUB)

    def send_message(self, msg: bytes) -> None:
        """Send a message (publish)."""
        logging.debug(log_msgs.SENDING_MESSAGE)
        if not self.pub:
            raise RuntimeError("publisher is not connected")

        future = self.pub.publish(self._topic_path, msg, retry=_DEFAULT_RETRY)
        logging.debug(f"Sent Message w/ Origin ID: {future.result()}")
        logging.debug(log_msgs.SENT_MESSAGE)


class GCPSub(GCP, Sub):
    """Wrapper around queue with prefetch-queue QoS.

    Extends:
        GCP
        Sub
    """

    def __init__(
        self, endpoint: str, project_id: str, topic_id: str, subscription_id: str
    ):
        logging.debug(
            f"{log_msgs.INIT_SUB} "
            f"({endpoint}; {project_id}; {topic_id}; {subscription_id})"
        )
        super().__init__(endpoint, project_id, topic_id)
        self.sub: Optional[pubsub.SubscriberClient] = None
        self.prefetch = 1

        self._subscription_path: Optional[str] = None
        self._subscription_id = subscription_id

    def connect(self) -> None:
        """Set up sub (subscriber) and create subscription if necessary.

        NOTE: Based on `examples/gcp/subscriber.create_subscription()`
        """
        logging.debug(log_msgs.CONNECTING_SUB)
        super().connect()

        self.sub, self._subscription_path = GCP._create_and_connect_sub(
            self.endpoint, self._project_id, self._topic_path, self._subscription_id
        )
        logging.debug(log_msgs.CONNECTED_SUB)

    def close(self) -> None:
        """Close sub."""
        logging.debug(log_msgs.CLOSING_SUB)
        super().close()
        if not self.sub:
            raise ClosingFailedExcpetion("No consumer to sub.")
        try:
            self.sub.close()
        except Exception as e:
            raise ClosingFailedExcpetion(str(e)) from e
        logging.debug(log_msgs.CLOSED_SUB)

    @staticmethod
    def _to_message(  # type: ignore[override]  # noqa: F821 # pylint: disable=W0221
        msg: pubsub.types.ReceivedMessage,  # pylint: disable=no-member
    ) -> Optional[Message]:
        """Transform GCP-Message to Message type."""
        return Message(msg.ack_id, msg.message.data)

    def _get_messages(
        self, timeout_millis: Optional[int], num_messages: int
    ) -> List[Message]:
        """Get n messages.

        The subscriber pulls a specific number of messages. The actual
        number of messages pulled may be smaller than `num_messages`.
        """
        if not self.sub:
            raise RuntimeError("subscriber is not connected")

        response = self.sub.pull(  # pylint: disable=no-member
            request={
                "subscription": self._subscription_path,
                "max_messages": num_messages,
            },
            retry=_DEFAULT_RETRY,
            # return_immediately=True, # NOTE - use is discourage for performance reasons
            timeout=timeout_millis / 1000 if timeout_millis else 0,
            # NOTE - if `retry` is specified, the timeout applies to each individual attempt
        )

        msgs = []
        for recvd in response.received_messages:
            logging.debug(f"Got Message w/ Origin ID: {recvd.message.message_id}")
            msg = GCPSub._to_message(recvd)
            if msg:
                msgs.append(msg)
        return msgs

    def get_message(
        self, timeout_millis: Optional[int] = TIMEOUT_MILLIS_DEFAULT
    ) -> Optional[Message]:
        """Get a message.

        NOTE: Based on `examples/gcp/subscriber.synchronous_pull()`
        """
        logging.debug(log_msgs.GETMSG_RECEIVE_MESSAGE)

        try:
            msg = self._get_messages(timeout_millis, 1)[0]
            logging.debug(f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({msg.msg_id!r}).")
            return msg
        except IndexError:  # NOTE - on timeout -> this will be len=0
            logging.debug(log_msgs.GETMSG_NO_MESSAGE)
            return None

    def _gen_messages(
        self, timeout_millis: Optional[int], num_messages: int
    ) -> Generator[Message, None, None]:
        """Continuously generate messages until there are no more."""
        while True:
            msgs = self._get_messages(timeout_millis, num_messages)
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
        self.sub.acknowledge(  # pylint: disable=no-member
            request={"subscription": self._subscription_path, "ack_ids": [msg.msg_id]}
        )
        logging.debug(f"{log_msgs.ACKED_MESSAGE} ({msg.msg_id!r}).")

    def reject_message(self, msg: Message) -> None:
        """Reject (nack) a message from the queue."""
        logging.debug(log_msgs.NACKING_MESSAGE)
        if not self.sub:
            raise RuntimeError("subscriber is not connected")

        # override the subscription-level ack deadline to fast-track redelivery
        self.sub.modify_ack_deadline(  # pylint: disable=no-member
            request={
                "subscription": self._subscription_path,
                "ack_ids": [msg.msg_id],
                "ack_deadline_seconds": 0,
            }
        )
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
    """GCP Pub-Sub Backend Factory.

    Extends:
        Backend
    """

    # NOTE - this could be an enviro var, but it is always constant across all members
    PROJECT_ID = "i3-gcp-proj"

    # NOTE - use single shared subscription
    # (making multiple unique subscription ids would create independent subscriptions)
    # See https://thecloudgirl.dev/images/pubsub.jpg
    SUBSCRIPTION_ID = "i3-gcp-sub"

    # NOTE - this is an environment variable, which should override the host address
    PUBSUB_EMULATOR_HOST = "PUBSUB_EMULATOR_HOST"

    @staticmethod
    def _figure_host_address(address: str) -> str:
        """If the pub-sub emulator enviro var is set, use that address."""
        try:
            emulator = os.environ[Backend.PUBSUB_EMULATOR_HOST]
            logging.warning(
                f"Environment variable `{Backend.PUBSUB_EMULATOR_HOST}` is set: "
                f"using Pub-Sub Emulator at {emulator} (overriding `{address}`)."
            )
            return emulator
        except KeyError:
            return address

    @staticmethod
    def create_pub_queue(address: str, name: str, auth_token: str = "") -> GCPPub:
        """Create a publishing queue.

        # NOTE - `auth_token` is not used currently
        """
        q = GCPPub(  # pylint: disable=invalid-name
            Backend._figure_host_address(address),
            Backend.PROJECT_ID,
            name,
            [f"{Backend.SUBSCRIPTION_ID}-{name}"],
        )
        q.connect()
        return q

    @staticmethod
    def create_sub_queue(
        address: str, name: str, prefetch: int = 1, auth_token: str = ""
    ) -> GCPSub:
        """Create a subscription queue.

        # NOTE - `auth_token` is not used currently
        """
        q = GCPSub(  # pylint: disable=invalid-name
            Backend._figure_host_address(address),
            Backend.PROJECT_ID,
            name,
            f"{Backend.SUBSCRIPTION_ID}-{name}",
        )
        q.prefetch = prefetch
        q.connect()
        return q

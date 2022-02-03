"""Run integration tests for NATS backend."""

import asyncio
import logging

import pytest
from mqclient.abstract_backend_tests import integrate_backend_interface, integrate_queue
from mqclient.abstract_backend_tests.utils import (  # pytest.fixture # noqa: F401 # pylint: disable=W0611
    queue_name,
)
from mqclient_nats.nats import Backend

logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger("flake8").setLevel(logging.WARNING)


@pytest.fixture(scope="module")
def event_loop():  # type: ignore[no-untyped-def]
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


class TestNATSQueue(integrate_queue.PubSubQueue):
    """Run PubSubQueue integration tests with NATS backend."""

    backend = Backend()


class TestNATSBackend(integrate_backend_interface.PubSubBackendInterface):
    """Run PubSubBackendInterface integration tests with NATS backend."""

    backend = Backend()

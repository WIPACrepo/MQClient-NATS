"""Run integration tests for GCP backend."""

import logging

from mqclient.implementation_tests import integrate_backend_interface, integrate_queue
from mqclient.implementation_tests.utils import (  # pytest.fixture # noqa: F401 # pylint: disable=W0611
    queue_name,
)
from mqclient_gcp.gcp import Backend

logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger("flake8").setLevel(logging.WARNING)


class TestGCPQueue(integrate_queue.PubSubQueue):
    """Run PubSubQueue integration tests with GCP backend."""

    backend = Backend()


class TestGCPBackend(integrate_backend_interface.PubSubBackendInterface):
    """Run PubSubBackendInterface integration tests with GCP backend."""

    backend = Backend()

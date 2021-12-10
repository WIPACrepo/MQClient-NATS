"""Queue class encapsulating a pub-sub messaging system with NATS."""


from typing import Any, cast

import mqclient

from . import nats


class Queue(mqclient.queue.Queue):
    __doc__ = mqclient.queue.Queue.__doc__

    def __init__(self, *args: Any, **kargs: Any) -> None:
        super().__init__(
            cast(
                mqclient.backend_interface.Backend, nats.Backend
            ),  # mypy is very picky
            *args,
            **kargs
        )

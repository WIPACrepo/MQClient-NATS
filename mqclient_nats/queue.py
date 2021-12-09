"""Queue class encapsulating a pub-sub messaging system with GCP."""


from typing import Any, cast

import mqclient

from . import gcp


class Queue(mqclient.queue.Queue):
    __doc__ = mqclient.queue.Queue.__doc__

    def __init__(self, *args: Any, **kargs: Any) -> None:
        super().__init__(
            cast(mqclient.backend_interface.Backend, gcp.Backend),  # mypy is very picky
            *args,
            **kargs
        )

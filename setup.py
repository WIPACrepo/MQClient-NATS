#!/usr/bin/env python
"""Setup."""

import os
import subprocess

from setuptools import setup  # type: ignore[import]

subprocess.run(
    "pip install git+https://github.com/WIPACrepo/wipac-dev-tools.git".split(),
    check=True,
)
from wipac_dev_tools import SetupShop  # noqa: E402  # pylint: disable=C0413

shop = SetupShop(
    "mqclient_gcp",
    os.path.abspath(os.path.dirname(__file__)),
    ((3, 6), (3, 9)),
    "Message Queue Client API with Google Cloud Platform (GCP)",
)

setup(
    url="https://github.com/WIPACrepo/MQClient-GCP",
    package_data={shop.name: ["py.typed", "requirements.txt"]},
    **shop.get_kwargs(),
)

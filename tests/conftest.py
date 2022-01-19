import logging
import sys

import pytest

sys.path.append("src/")
from airbyte import AirbyteConnector


@pytest.fixture
def airbyte_instance():

    logger = logging.getLogger()

    airbyte_connector = AirbyteConnector(
        logger,
        airbyte_connection_id="1234",
        airbyte_retries=5,
        airbyte_waiting_time=10,
        airbyte_max_time=7200,
        airbyte_endpoint="http://localhost:8080",
        airbyte_source_start_date=None,
    )
    return airbyte_connector

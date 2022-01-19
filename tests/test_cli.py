import sys
from typing import Dict
from typing import Tuple

import pytest

sys.path.append("src/")
from cli import cli

CLI_PARAMS = [
    ("-c", "1234", "-v"),
    (
        "--airbyte-connection-id",
        "1234",
        "--airbyte-waiting-time",
        "1",
        "--airbyte-retries",
        "2",
        "--airbyte-max-time",
        "200",
    ),
]
EXPECTED_PARAMS = [
    {
        "airbyte_connection_id": "1234",
        "verbose": True,
        "airbyte_waiting_time": 10,
        "airbyte_retries": 5,
        "airbyte_max_time": 7200,
        "airbyte_source_start_date": None,
    },
    {
        "airbyte_connection_id": "1234",
        "verbose": False,
        "airbyte_waiting_time": 1,
        "airbyte_retries": 2,
        "airbyte_max_time": 200,
        "airbyte_source_start_date": None,
    },
]


@pytest.mark.parametrize("args, expected", zip(CLI_PARAMS, EXPECTED_PARAMS))
def test_cli(args: Tuple, expected: Dict):
    cli_params = cli(args)

    assert cli_params == expected

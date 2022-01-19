import argparse
from typing import Dict
from typing import List


def cli(args: List) -> Dict:

    parser = argparse.ArgumentParser("Utility for dealing with the Airbyte API")

    parser.add_argument("-c", "--airbyte-connection-id", help="The connection id to use", required=True)

    parser.add_argument(
        "-w", "--airbyte-waiting-time", help="Waiting time between Airbyte requests", default=10, type=int
    )

    parser.add_argument("-r", "--airbyte-retries", help="Number of retries before giving up", default=5, type=int)

    parser.add_argument(
        "-m", "--airbyte-max-time", help="Maximum time to wait for Airbyte to respond", default=7200, type=int
    )

    parser.add_argument("-v", "--verbose", help="Verbose output", action="store_true")

    parser.add_argument("-s", "--airbyte-source-start-date", help="Source connector start date", default=None)

    return parser.parse_args(args).__dict__

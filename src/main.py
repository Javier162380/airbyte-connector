import logging
import sys

from airbyte import AirbyteConnector
from cli import cli


def main():
    # parsing cli params.
    cli_params = cli(sys.argv[1:])

    # setting logger.
    logger = logging.getLogger()
    logging_verbosity = cli_params.pop("verbose")
    logging.basicConfig(
        stream=sys.stdout,
        format="%(asctime)s [%(threadName)-12.12s] " "[%(levelname)-5.5s]  %(message)s",
    )
    logger.setLevel(logging.DEBUG if logging_verbosity else logging.INFO)

    airbyte_instance = AirbyteConnector(logger=logger, **cli_params)
    airbyte_instance.run()


if __name__ == "__main__":
    main()

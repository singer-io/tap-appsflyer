import json
import sys

import singer

from tap_appsflyer.client import Client
from tap_appsflyer.discover import discover
from tap_appsflyer.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["app_id", "api_token"]


def do_discover():
    LOGGER.info("Starting discover")
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")


@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    state = {}
    if parsed_args.state:
        state = parsed_args.state

    with Client(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            sync(
                client=client,
                config=parsed_args.config,
                catalog=parsed_args.catalog,
                state=state,
            )


if __name__ == "__main__":
    main()

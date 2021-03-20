#!/usr/bin/env python3
import json

import singer
from singer.catalog import write_catalog
from .discover import discover
from .sync import sync

LOGGER = singer.get_logger()

@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args([])

    catalog = args.catalog if args.catalog else discover()
    
    if args.discover:
        write_catalog(catalog)
    else:
        sync(args.config, args.state, catalog)

if __name__ == '__main__':
    main()
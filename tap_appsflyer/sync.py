from typing import Dict

import singer

from tap_appsflyer.client import Client
from tap_appsflyer.streams import STREAMS

LOGGER = singer.get_logger()


def update_currently_syncing(state: Dict, stream_name: str) -> None:
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(client: Client, config: Dict, catalog: singer.Catalog, state) -> None:
    """Sync selected streams from catalog."""

    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info(f"selected_streams: {selected_streams}")

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info(f"last/currently syncing stream: {last_stream}")

    with singer.Transformer() as transformer:
        for stream_name in selected_streams:

            stream = STREAMS[stream_name](client)
            stream_catalog = catalog.get_stream(stream_name)
            stream_schema = stream_catalog.schema.to_dict()
            stream_metadata = singer.metadata.to_map(stream_catalog.metadata)

            stream.write_schema(stream_schema, stream_name)

            LOGGER.info(f"START Syncing: {stream_name}")
            update_currently_syncing(state, stream_name)
            total_records = stream.sync(
                state=state,
                schema=stream_schema,
                stream_metadata=stream_metadata,
                transformer=transformer,
            )

            update_currently_syncing(state, None)
            LOGGER.info(
                f"FINISHED Syncing: {stream_name}, total_records: {total_records}"
            )

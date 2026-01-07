from typing import Dict

import singer

from tap_trello.client import Client
from tap_trello.streams import STREAMS
from tap_trello.streams.abstracts import LegacyStream

LOGGER = singer.get_logger()


def _instantiate_stream(stream_class, client, catalog_entry, config, state):
    """Instantiate a stream class handling legacy and latest constructors.

    LegacyStream classes expect (client, config, state).
    Latest streams expect (client, catalog_entry).
    """
    try:
        if isinstance(stream_class, type) and issubclass(stream_class, LegacyStream):
            return stream_class(client, config, state)
    except Exception:
        # If any problem with issubclass check, fall back to latest constructor
        pass

    return stream_class(client, catalog_entry)


def update_currently_syncing(state: Dict, stream_name: str) -> None:
    """
    Update currently_syncing in state and write it
    """
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def write_schema(stream, client, streams_to_sync, catalog, config=None, state=None) -> None:
    """
    Write schema for stream and its children
    """
    # Handle both latest streams and legacy streams
    if hasattr(stream, 'is_selected') and callable(getattr(stream, 'is_selected')):
        if stream.is_selected():
            stream.write_schema()
    else:
        # Legacy stream: write schema manually using catalog
        stream_id = getattr(stream, 'tap_stream_id', None) or getattr(stream, 'stream_id', None)
        if stream_id and stream_id in streams_to_sync:
            try:
                catalog_entry = catalog.get_stream(stream_id)
                schema_obj = getattr(catalog_entry, 'schema', None)
                schema_dict = schema_obj.to_dict() if hasattr(schema_obj, 'to_dict') else schema_obj
                key_props = getattr(stream, 'key_properties', None) or ["id"]
                singer.write_schema(stream_id, schema_dict, key_props)
            except Exception:
                LOGGER.debug("Could not write schema for stream: %s", stream_id)

    # Handle children if the stream has any
    children = getattr(stream, 'children', []) or []
    for child in children:
        child_class = STREAMS[child]
        child_obj = _instantiate_stream(child_class, client, catalog.get_stream(child), config, state)
        write_schema(child_obj, client, streams_to_sync, catalog, config, state)
        if child in streams_to_sync:
            if not hasattr(stream, 'child_to_sync'):
                stream.child_to_sync = []
            stream.child_to_sync.append(child_obj)


def sync(client: Client, config: Dict, catalog: singer.Catalog, state) -> None:
    """
    Sync selected streams from catalog
    """
    streams_to_sync = []
    for stream in catalog.get_selected_streams(state):
        streams_to_sync.append(stream.stream)
    LOGGER.info("selected_streams: {}".format(streams_to_sync))

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: {}".format(last_stream))

    with singer.Transformer() as transformer:
        for stream_name in streams_to_sync:
            stream_class = STREAMS[stream_name]

            # Check if stream has a parent - child streams need special handling
            parent_attribute = getattr(stream_class, 'parent', None)
            if parent_attribute:
                parent_id = None
                if isinstance(parent_attribute, str):
                    parent_id = parent_attribute
                elif isinstance(parent_attribute, type) and hasattr(parent_attribute, 'stream_id'):
                    parent_id = parent_attribute.stream_id

                # Skip child stream if parent is not selected
                if parent_id and parent_id not in streams_to_sync:
                    LOGGER.info("Skipping stream: {}".format(stream_name))
                    continue

            stream = _instantiate_stream(stream_class, client, catalog.get_stream(stream_name), config, state)

            write_schema(stream, client, streams_to_sync, catalog, config, state)
            LOGGER.info("START Syncing: {}".format(stream_name))
            update_currently_syncing(state, stream_name)

            if isinstance(stream, LegacyStream):
                # Legacy streams: sync() returns generator, manually write records
                catalog_entry = catalog.get_stream(stream_name)
                schema_obj = getattr(catalog_entry, 'schema', None)
                schema_dict = schema_obj.to_dict() if hasattr(schema_obj, 'to_dict') else schema_obj
                metadata_list = getattr(catalog_entry, 'metadata', None)

                metadata_map = singer.metadata.to_map(metadata_list) if metadata_list else {}

                with singer.metrics.record_counter(stream_name) as counter:
                    for rec in stream.sync():
                        transformed_record = transformer.transform(rec, schema_dict, metadata_map)
                        singer.write_record(stream_name, transformed_record)
                        counter.increment()
                total_records = counter.value
            else:
                # Latest streams: sync() handles everything and returns count
                if parent_attribute:
                    parent_id = None
                    if isinstance(parent_attribute, str):
                        parent_id = parent_attribute
                    elif isinstance(parent_attribute, type) and hasattr(parent_attribute, 'stream_id'):
                        parent_id = parent_attribute.stream_id

                    if parent_id and parent_id in STREAMS:
                        parent_class = STREAMS[parent_id]
                        parent_stream = _instantiate_stream(parent_class, client, catalog.get_stream(parent_id), config, state)

                        total_records = 0
                        if isinstance(parent_stream, LegacyStream):
                            parent_iter = parent_stream.sync()
                        else:
                            parent_iter = parent_stream.get_records()

                        for parent_obj in parent_iter:
                            total_records += stream.sync(state=state, transformer=transformer, parent_obj=parent_obj)
                    else:
                        total_records = stream.sync(state=state, transformer=transformer)
                else:
                    # Not a child stream, sync normally
                    total_records = stream.sync(state=state, transformer=transformer)

            update_currently_syncing(state, None)
            LOGGER.info(
                "FINISHED Syncing: {}, total_records: {}".format(
                    stream_name, total_records
                )
            )

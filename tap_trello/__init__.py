import json
import os

import singer
from singer import utils, metadata
from singer.catalog import Catalog, write_catalog
import tap_trello.streams as streams
from tap_trello.client import TrelloClient

LOGGER = singer.get_logger()


STREAM_OBJECTS = {'foo': streams.Foo}


def _get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def _load_schemas():
    schemas = {}

    for filename in os.listdir(_get_abs_path("schemas")):
        path = _get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def do_discover():
    raw_schemas = _load_schemas()
    catalog_entries = []

    for stream_name, schema in raw_schemas.items():
        # create and add catalog entry
        stream = STREAM_OBJECTS[stream_name]
        catalog_entry = {
            "stream": stream_name,
            "tap_stream_id": stream_name,
            "schema": schema,
            "metadata": metadata.get_standard_metadata(
                schema=schema,
                key_properties=stream.key_properties,
                valid_replication_keys=stream.replication_keys,
                replication_method=stream.replication_method,
            ),
            "key_properties": stream.key_properties,
        }
        catalog_entries.append(catalog_entry)

    return Catalog.from_dict({"streams": catalog_entries})


def do_sync():
    LOGGER.warning("Sync not implemented")


@utils.handle_top_exception(LOGGER)
def main():
    required_config_keys = ['start_date', 'consumer_key', 'consumer_secret', 'access_token', 'access_token_secret']
    args = singer.parse_args(required_config_keys)

    config = args.config  # pylint:disable=unused-variable
    client = TrelloClient(config)  # pylint:disable=unused-variable
    catalog = args.catalog or Catalog([])
    state = args.state # pylint:disable=unused-variable

    if args.properties and not args.catalog:
        raise Exception("DEPRECATED: Use of the 'properties' parameter is not supported. Please use --catalog instead")

    if args.discover:
        LOGGER.info("Starting discovery mode")
        catalog = do_discover()
        write_catalog(catalog)
    else:
        do_sync()

if __name__ == "__main__":
    main()

import singer
from singer import utils
from singer.catalog import Catalog, write_catalog
from tap_trello.discover import do_discover
from tap_trello.sync import do_sync
import tap_trello.streams as streams
from tap_trello.client import TrelloClient

LOGGER = singer.get_logger()

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
        do_sync(client, config, state, catalog)

if __name__ == "__main__":
    main()

import singer
from singer import utils
from singer.catalog import Catalog

LOGGER = singer.get_logger()

def do_discover():
    pass

def do_sync():
    LOGGER.warning("Sync not implemented")

@utils.handle_top_exception(LOGGER)
def main():
    required_config_keys = ['start_date']
    args = singer.parse_args(required_config_keys)

    config = args.config
#    client = Client(config)
    catalog = args.catalog or Catalog([])
    state = args.state

    if args.properties and not args.catalog:
        raise Exception("DEPRECATED: Use of the 'properties' parameter is not supported. Please use --catalog instead")

    if args.discover:
        do_discover()
    else:
        do_sync()

if __name__ == "__main__":
    main()

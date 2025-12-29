import singer

from tap_trello.streams.abstracts import AddCustomFields, ChildStream
from tap_trello.streams.boards import Boards

LOGGER = singer.get_logger()


class Cards(AddCustomFields, ChildStream):
    stream_id = "cards"
    stream_name = "cards"
    endpoint = "/boards/{}/cards/all"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    parent = Boards
    MAX_API_RESPONSE_SIZE = 1000

    def get_records(self, format_values, additional_params=None):
        # Get max_api_response_size from config and set to parameter
        cards_response_size = self.config.get('cards_response_size')
        self.MAX_API_RESPONSE_SIZE = int(cards_response_size) if cards_response_size else self.MAX_API_RESPONSE_SIZE
        self.params = {'limit': self.MAX_API_RESPONSE_SIZE, 'customFieldItems': 'true'}

        # Set window_end with current time
        window_end = singer.utils.strftime(singer.utils.now())

        # Build custom fields and dropdown object map for the specific parent
        custom_fields_map, dropdown_options_map = self.build_custom_fields_maps(parent_id_list=format_values)

        while True:

            # Get records for cards before specified time
            # Reference: https://developer.atlassian.com/cloud/trello/guides/rest-api/api-introduction/#paging
            records = self.client.get(self._format_endpoint(format_values), params={"before": window_end,
                                                                                   **self.params})

            # Raise exception if API returns more data than specified limit
            if self.MAX_API_RESPONSE_SIZE and len(records) > self.MAX_API_RESPONSE_SIZE:
                raise Exception(
                    ("{}: Number of records returned is greater than the requested API response size of {}.").format(
                        self.stream_id,
                        self.MAX_API_RESPONSE_SIZE)
                )

            # Yielding records after adding custom fields and dropdown object map to all records
            for rec in records:
                yield self.modify_record(rec, parent_id_list = format_values, custom_fields_map = custom_fields_map, dropdown_options_map = dropdown_options_map)

            LOGGER.info("%s - Collected  %s records for board %s.",
                        self.stream_id,
                        len(records),
                        format_values[0])

            # If records are same as limit then shift window to get older data
            if len(records) == self.MAX_API_RESPONSE_SIZE:
                # Sort cards based on card ID as API returns latest records but in unordered manner
                records = sorted(records, key=lambda x: x['id'])
                # API returns latest records so set window_end to smallest card id to get older data
                window_end = records[0]["id"]
            else:
                # API returns less records than limit, break the pagination
                break

import singer

from tap_trello.streams.abstracts import ChildStream

LOGGER = singer.get_logger()


class Cards(ChildStream):
    stream_id = "cards"
    stream_name = "cards"
    endpoint = "/boards/{}/cards/all"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    parent = "boards"
    MAX_API_RESPONSE_SIZE = 1000

    def _get_dropdown_option_key(self, field_id, option_id):
        """Generate a unique key for dropdown options."""
        return field_id + '_' + option_id

    def build_custom_fields_maps(self, **kwargs):
        """Build maps of custom field IDs to names and dropdown option IDs to values."""
        custom_fields_map = {}
        dropdown_options_map = {}
        board_id_list = kwargs['parent_id_list']
        # The custom fields are defined on the board level, so this function is called on a per-board basis
        # Therefore, we validate that only one board is being passed in
        if len(board_id_list) != 1:
            raise ValueError(f"Expected exactly one board ID, got {len(board_id_list)}")
        custom_fields = self.client.get('/boards/{}/customFields'.format(board_id_list[0]))
        for custom_field in custom_fields:
            custom_fields_map[custom_field['id']] = custom_field['name']
            if custom_field['type'] == 'list':
                for dropdown_option in custom_field['options']:
                    dropdown_option_key = self._get_dropdown_option_key(dropdown_option['idCustomField'], dropdown_option['id'])
                    dropdown_options_map[dropdown_option_key] = dropdown_option['value']['text']

        return custom_fields_map, dropdown_options_map

    def modify_record(self, record, **kwargs):
        """Add custom field names and dropdown values to card records."""
        custom_fields_map = kwargs['custom_fields_map']
        dropdown_options_map = kwargs['dropdown_options_map']
        for custom_field in record['customFieldItems']:
            custom_field['name'] = custom_fields_map[custom_field['idCustomField']]
            if custom_field.get('idValue', None):
                dropdown_option_key = self._get_dropdown_option_key(custom_field['idCustomField'], custom_field['idValue'])
                custom_field['value'] = {'option': dropdown_options_map[dropdown_option_key]}

        return record

    def get_records(self, format_values, additional_params=None):
        # Get max_api_response_size from config and set to parameter
        cards_response_size = int(self.config.get('cards_response_size') or self.MAX_API_RESPONSE_SIZE)
        self.MAX_API_RESPONSE_SIZE = min(cards_response_size, 1000)
        self.params = {'limit': self.MAX_API_RESPONSE_SIZE, 'customFieldItems': 'true'}

        # Set window_end with current time
        window_end = singer.utils.strftime(singer.utils.now())

        # Build custom fields and dropdown object map for the specific parent
        custom_fields_map, dropdown_options_map = self.build_custom_fields_maps(parent_id_list=format_values)

        has_more_pages = True
        while has_more_pages:

            # Get records for cards before specified time
            # Reference: https://developer.atlassian.com/cloud/trello/guides/rest-api/api-introduction/#paging
            records = self.client.get(self._format_endpoint(format_values), params={"before": window_end,
                                                                                   **self.params})

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
                # API returns less records than limit, stop pagination
                has_more_pages = False

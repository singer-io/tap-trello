import json
import singer

from tap_trello.streams.abstracts import FullTableStream

LOGGER = singer.get_logger()


class Members(FullTableStream):
    tap_stream_id = "members"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "/members/{id}"
    parent = "users"

    def get_records(self):
        """
        Override to support this endpoint which returns a single dict for a single record.
        """
        url = self.url_endpoint or self.get_url_endpoint(getattr(self, 'parent_obj', None))
        response = self.client.make_request(
            self.http_method,
            url,
            self.params,
            self.headers,
            body=json.dumps(self.data_payload),
            path=self.path
        )

        raw_records, _ = self._normalize_response(response, url)
        yield from raw_records

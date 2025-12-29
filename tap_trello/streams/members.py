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
        response = self.client.make_request(
            self.http_method,
            self.url_endpoint or self.get_url_endpoint(getattr(self, 'parent_obj', None)),
            self.params,
            self.headers,
            body=json.dumps(self.data_payload),
            path=self.path
        )

        if isinstance(response, dict):
            yield response
        elif isinstance(response, list):
            for record in response:
                yield record
        else:
            LOGGER.warning(
                "%s - Unexpected response type %s from endpoint %s",
                getattr(self, 'tap_stream_id', str(self.__class__)),
                type(response),
                self.url_endpoint
            )

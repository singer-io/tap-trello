import json
import singer

from tap_trello.streams.abstracts import ChildBaseStream
from tap_trello.exceptions import TrelloBadRequestError

LOGGER = singer.get_logger()


class OrganizationActions(ChildBaseStream):
    tap_stream_id = "organization_actions"
    key_properties = ["id", "organization_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date"]
    path = "/organizations/{id}/actions"
    parent = "organizations"
    bookmark_value = None


    def get_records(self):
        url = self.get_url_endpoint(getattr(self, 'parent_obj', None)) if hasattr(self, 'parent_obj') else self.get_url_endpoint()

        params = dict(self.params) if hasattr(self, 'params') else {}
        params.pop('page', None)

        response = self.client.make_request(
            self.http_method,
            url,
            params,
            self.headers,
            body=json.dumps(self.data_payload),
            path=self.path
        )

        if isinstance(response, dict):
            raw_records = response.get(self.data_key, [])
        elif isinstance(response, list):
            raw_records = response
        else:
            LOGGER.warning(
                "%s - Unexpected response type %s from endpoint %s",
                getattr(self, 'tap_stream_id', str(self.__class__)),
                type(response),
                url,
            )
            raw_records = []
        yield from raw_records

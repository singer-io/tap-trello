import json
import singer

from tap_trello.streams.abstracts import ChildBaseStream

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
            params=params,
            headers=self.headers,
            body=json.dumps(self.data_payload),
            path=self.path,
        )

        raw_records, _ = self._normalize_response(response, url)
        yield from raw_records

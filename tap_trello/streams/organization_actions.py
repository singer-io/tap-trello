import json
from typing import Dict

import singer
from singer import Transformer

from tap_trello.streams.abstracts import ChildBaseStream

LOGGER = singer.get_logger()


class OrganizationActions(ChildBaseStream):
    tap_stream_id = "organization_actions"
    key_properties = ["id", "organization_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date"]
    path = "/organizations/{id}/actions"
    parent = "organizations"
    params = {'limit': 1000}

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Override sync to store state and add date filtering."""
        self._sync_state = state
        self._sync_parent_obj = parent_obj

        return super().sync(state, transformer, parent_obj)

    def get_records(self):
        """Get records with date filtering for incremental replication."""
        url = self.get_url_endpoint(getattr(self, '_sync_parent_obj', None))
        params = dict(self.params) if hasattr(self, 'params') else {}
        params.pop('page', None)

        # Add date filtering for incremental replication
        # Access state from the temporarily stored sync state
        state = getattr(self, '_sync_state', {})
        bookmark_date = self.get_bookmark(state, self.tap_stream_id)
        if bookmark_date:
            params['since'] = bookmark_date
        else:
            # Use start_date from config if no bookmark exists
            start_date = self.client.config.get('start_date')
            if start_date:
                params['since'] = start_date

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

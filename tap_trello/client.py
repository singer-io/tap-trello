import requests
from requests_oauthlib import OAuth1

import singer
import backoff

LOGGER = singer.get_logger()

ENDPOINT_BASE = "https://api.trello.com/1"

class TrelloClient():
    def __init__(self, config):
        self.oauth = OAuth1(config['consumer_key'],
                            client_secret=config['consumer_secret'],
                            resource_owner_key=config['access_token'],
                            resource_owner_secret=config['access_token_secret'],
                            signature_method='HMAC-SHA1')
        self.member_id = self._get_member_id()

    def _get_member_id(self):
        return self.get('/members/me')['id']

    @backoff.on_exception(backoff.constant,
                          (requests.exceptions.HTTPError),
                          max_tries=3,
                          interval=10)
    def _make_request(self, method, endpoint, headers=None, params=None):
        full_url = ENDPOINT_BASE + endpoint
        LOGGER.info(
            "%s - Making request to %s endpoint %s, with params %s",
            full_url,
            method.upper(),
            endpoint,
            params,
        )

        # NB: There are headers defining Trello version and Trello
        # environment (e.g., Production) here, might be useful in the
        # future. At initial development, version is 1.2083.0
        response = requests.request(method, full_url, headers=headers, params=params, auth=self.oauth)

        response.raise_for_status()
        # TODO: Check error status, rate limit, etc.
        return response.json()

    def get(self, url, headers=None, params=None):
        return self._make_request("GET", url, headers=headers, params=params)

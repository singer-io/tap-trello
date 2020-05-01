import requests
from requests_oauthlib import OAuth1

class TrelloClient():
    def __init__(self, config):
        self.oauth = OAuth1(config['consumer_key'],
                            client_secret=config['consumer_secret'],
                            resource_owner_key=config['access_token'],
                            resource_owner_secret=config['access_token_secret'],
                            signature_method='HMAC-SHA1')
        self.member_id = self._get_member_id()

    def _get_member_id(self):
        return self.get('https://api.trello.com/1/members/me')['id']

    def _make_request(self, method, url, headers=None, params=None):
        response = requests.request(method, url, headers=headers, params=params, auth=self.oauth)
        # TODO: Check error status, rate limit, etc.
        return response.json()

    def get(self, url, headers=None, params=None):
        return self._make_request("GET", url, headers=headers, params=params)

from typing import Any, Dict, Mapping, Optional

import backoff
import requests

from requests import session
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout # pylint: disable=redefined-builtin

from singer import get_logger, metrics
from tap_trello.exceptions import (ERROR_CODE_EXCEPTION_MAPPING,
                                   TrelloError,
                                   TrelloBackoffError, TrelloRateLimitError)

LOGGER = get_logger()
REQUEST_TIMEOUT = 300


def raise_for_error(response: requests.Response) -> None:
    """Raises the associated response exception. Takes in a response object,
    checks the status code, and throws the associated exception based on the
    status code.

    :param resp: requests.Response object
    """
    try:
        response_json = response.json()
    except Exception:
        response_json = {}
    if response.status_code not in [200, 201, 204]:
        mapping = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {})
        if response_json.get("error"):
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('error')}"
        else:
            # For unmapped 5xx, provide a meaningful default message
            if not mapping and 500 <= response.status_code < 600:
                default_message = "An unexpected server error occurred."
            else:
                default_message = "Unknown Error"
            error_message = mapping.get("message", default_message)
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('message', error_message)}"

        exc = mapping.get("raise_exception", TrelloError)

        # For 5xx errors, use backoff exception if not specifically mapped
        if 500 <= response.status_code < 600:
            exc = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get(
                "raise_exception", TrelloBackoffError
            )
        else:
            exc = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get(
                "raise_exception", TrelloError
            )
        raise exc(message, response) from None


class Client:
    """
    A Wrapper class.
    ~~~
    Performs:
     - Authentication
     - Response parsing
     - HTTP Error handling and retry
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self._session = session()
        self.base_url = "https://api.trello.com/1"
        config_request_timeout = config.get("request_timeout")
        self.request_timeout = float(config_request_timeout) if config_request_timeout else REQUEST_TIMEOUT
        self._member_id = None

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    def _get_member_id(self):
        resp = self.get('/members/me')
        if isinstance(resp, dict):
            return resp.get('id')
        return None

    def make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        path: Optional[str] = None
    ) -> Any:
        """
        Sends an HTTP request to the specified API endpoint.
        """
        params = params or {}
        headers = headers or {}
        body = body or {}
        endpoint = endpoint or f"{self.base_url}/{path}"
        params["key"] = self.config["api_key"]
        params["token"] = self.config["api_token"]
        return self.__make_request(
            method, endpoint,
            headers=headers,
            params=params,
            data=body,
            timeout=self.request_timeout
        )

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            ConnectionResetError,
            ConnectionError,
            ChunkedEncodingError,
            Timeout,
            TrelloBackoffError,
            TrelloRateLimitError
        ),
        max_tries=5,
        factor=2,
    )
    def __make_request(
        self, method: str, endpoint: str, **kwargs
    ) -> Optional[Mapping[Any, Any]]:
        """Performs HTTP Operations."""
        method = method.upper()
        with metrics.http_request_timer(endpoint):
            if method in ("GET", "POST"):
                if method == "GET":
                    kwargs.pop("data", None)
                response = self._session.request(method, endpoint, **kwargs)
                raise_for_error(response)
            else:
                raise ValueError(f"Unsupported method: {method}")

        return response.json()

    def get(self, path, headers=None, params=None):
        """Helper method for GET requests (used by legacy streams)."""
        return self.make_request('GET', None, params=params or {}, headers=headers or {}, path=path)

    @property
    def member_id(self) -> Any:
        if self._member_id:
            return self._member_id

        try:
            self._member_id = self._get_member_id()
        except Exception:
            self._member_id = None

        return self._member_id

class TrelloError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class TrelloBackoffError(TrelloError):
    """class representing backoff error handling."""
    pass

class TrelloBadRequestError(TrelloError):
    """class representing 400 status code."""
    pass

class TrelloUnauthorizedError(TrelloError):
    """class representing 401 status code."""
    pass


class TrelloForbiddenError(TrelloError):
    """class representing 403 status code."""
    pass

class TrelloNotFoundError(TrelloError):
    """class representing 404 status code."""
    pass

class TrelloConflictError(TrelloError):
    """class representing 409 status code."""
    pass

class TrelloUnprocessableEntityError(TrelloError):
    """class representing 422 status code."""
    pass

class TrelloRateLimitError(TrelloBackoffError):
    """class representing 429 status code."""
    pass

class TrelloInternalServerError(TrelloBackoffError):
    """class representing 500 status code."""
    pass

class TrelloNotImplementedError(TrelloBackoffError):
    """class representing 501 status code."""
    pass

class TrelloBadGatewayError(TrelloBackoffError):
    """class representing 502 status code."""
    pass

class TrelloServiceUnavailableError(TrelloBackoffError):
    """class representing 503 status code."""
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": TrelloBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": TrelloUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": TrelloForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": TrelloNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": TrelloConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": TrelloUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": TrelloRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": TrelloInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": TrelloNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": TrelloBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": TrelloServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}

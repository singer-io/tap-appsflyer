class appsflyerError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class appsflyerBackoffError(appsflyerError):
    """class representing backoff error handling."""
    pass

class appsflyerBadRequestError(appsflyerError):
    """class representing 400 status code."""
    pass

class appsflyerUnauthorizedError(appsflyerError):
    """class representing 401 status code."""
    pass


class appsflyerForbiddenError(appsflyerError):
    """class representing 403 status code."""
    pass

class appsflyerNotFoundError(appsflyerError):
    """class representing 404 status code."""
    pass

class appsflyerConflictError(appsflyerError):
    """class representing 406 status code."""
    pass

class appsflyerUnprocessableEntityError(appsflyerBackoffError):
    """class representing 409 status code."""
    pass

class appsflyerRateLimitError(appsflyerBackoffError):
    """class representing 429 status code."""
    pass

class appsflyerInternalServerError(appsflyerBackoffError):
    """class representing 500 status code."""
    pass

class appsflyerNotImplementedError(appsflyerBackoffError):
    """class representing 501 status code."""
    pass

class appsflyerBadGatewayError(appsflyerBackoffError):
    """class representing 502 status code."""
    pass

class appsflyerServiceUnavailableError(appsflyerBackoffError):
    """class representing 503 status code."""
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": appsflyerBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": appsflyerUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": appsflyerForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": appsflyerNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": appsflyerConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": appsflyerUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": appsflyerRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": appsflyerInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": appsflyerNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": appsflyerBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": appsflyerServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}

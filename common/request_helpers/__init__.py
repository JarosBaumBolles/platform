"""
This package has all helper logic related to HTTP ReST, RPC, websockets and any networking logic.
Decoding from JSON, XML, CSV is performed as part of querying logic as well.
"""

# Standard imports
import logging
import time
import uuid
import re
from enum import Enum
from functools import wraps
from json import dumps, loads
from urllib import parse
from urllib.request import (
    HTTPBasicAuthHandler,
    HTTPPasswordMgrWithDefaultRealm,
    Request,
    build_opener,
    install_opener,
    urlopen,
)

from common.logging import Logger

# Assuming that an HTTP (or HTTPS) address :
# starts with "http://" or "https://"
# contains at least one "." between the TLD and the domain name
# the domain name is composed of letters, numbers _ and -
# the URL is delimited at the end by a space and can contain any other character

URL_MATCH = re.compile(r"(http|https)://[\w\-]+(\.[\w\-]+)+\S*")

LOGGER = Logger(
    name="DEPLOYMENT",
    level="DEBUG",
    description="DEPLOYMENT",
    trace_id=uuid.uuid4(),
)


class HTTPRequestMethod(Enum):
    """HTTP methods represented as enum."""

    GET = "GET"
    POST = "POST"


class PayloadType(Enum):
    """HTTP payload type represented as enum."""

    TEXT = 0
    JSON = 1


class HTTPCompressionSupport(Enum):
    """HTTP compression algorithm represented as enum."""

    GZIP = 0


class JBBRequestHelperException(Exception):
    """Exception class specific to this package."""


TIMEOUT = 30


def http_request(  # pylint:disable=too-many-arguments
    url,
    parameters=None,
    payload=None,
    headers=None,
    method=HTTPRequestMethod.GET,
    request_payload_type=PayloadType.JSON,
    response_payload_type=PayloadType.TEXT,
):
    """Wrapper around standard urllib logic to help perform HTTP request related logic."""

    # assert isinstance(url, str)
    # assert isinstance(parameters, dict) or parameters is None
    # assert isinstance(method, HTTPRequestMethod)
    # assert isinstance(request_payload_type, PayloadType)
    # assert isinstance(response_payload_type, PayloadType)

    if payload:
        if request_payload_type != PayloadType.JSON:
            raise JBBRequestHelperException(
                f"Request payload is not supported yet: {request_payload_type}"
            )
        # assert isinstance(payload, (dict, list))
        payload = dumps(payload).encode("utf-8")

    if parameters:
        url = f"{url}?{parse.urlencode(parameters)}"
    if not URL_MATCH.match(url):
        raise JBBRequestHelperException(
            f"The given URI '{url}' doe not match to the url format expecttions:\n"
            ' - starts with "http://" or "https://"'
            ' - contains at least one "." between the TLD and the domain name'
            " - the domain name is composed of letters, numbers _ and -"
            " - the URL is delimited at the end by a space and can contain any"
            "   character"
        )
    url_request = Request(
        url=url, data=payload, method=method.name, headers=(headers or {})
    )
    with urlopen(url_request, timeout=TIMEOUT) as response:  # nosec

        if response.getcode() != 200:
            LOGGER.error("Downtime happened on the integration provider side")

        raw_response = response.read()
        if response_payload_type == PayloadType.TEXT:
            return raw_response.decode("utf-8")
        if response_payload_type == PayloadType.JSON:
            return loads(raw_response)

        raise JBBRequestHelperException(
            f"Request payload is not supported yet: {request_payload_type}"
        )


def set_up_basic_authentication(url, username, password):
    """Wrapper created to set up basic authentication where we need it in our requests."""

    password_manager = HTTPPasswordMgrWithDefaultRealm()

    # Add the username and password
    # If we knew the realm, we could use it instead of None
    password_manager.add_password(None, url, username, password)

    handler = HTTPBasicAuthHandler(password_manager)

    opener = build_opener(handler)

    # Use the opener to fetch a URL
    opener.open(url)

    # Now all calls to urllib.request.urlopen use our opener
    install_opener(opener)


def retry(exceptions, tries=4, delay=1, backoff=2):
    """
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: Exception to check for. May be a tuple of
            multiple exceptions.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
    """

    def decorator_function(func):
        @wraps(func)
        def f_retry(*args, **kwargs):
            tries_count, delay_time = tries, delay
            while tries_count > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions as err:
                    LOGGER.error(f"{err}, Retrying in {delay_time} seconds...")
                    time.sleep(delay_time)
                    tries_count -= 1
                    delay_time *= backoff
            return func(*args, **kwargs)

        return f_retry

    return decorator_function

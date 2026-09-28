import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Default retry policy for adapters' extract-path HTTP calls. Tune here to
# change retry behavior pipeline-wide.
DEFAULT_TOTAL_RETRIES = 5
DEFAULT_BACKOFF_FACTOR = (
    2  # urllib3: backoff_factor * (2 ** (retry_count - 1)), capped by backoff_max
)
DEFAULT_BACKOFF_MAX_SECONDS = 60
DEFAULT_STATUS_FORCELIST = (429, 500, 502, 503, 504)
DEFAULT_ALLOWED_METHODS = frozenset(
    {"GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE", "POST"}
)
# Applied only when a call site doesn't pass its own timeout= - requests
# itself never applies a timeout implicitly, so this is a safety net against
# a call hanging indefinitely.
DEFAULT_TIMEOUT_SECONDS = 300


class _DefaultTimeoutHTTPAdapter(HTTPAdapter):
    """
    HTTPAdapter that falls back to a default timeout for any request that
    doesn't specify its own timeout=... .
    """

    def __init__(self, *args, default_timeout: float, **kwargs):
        self.default_timeout = default_timeout
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = self.default_timeout
        return super().send(request, **kwargs)


def build_retrying_session(
    total_retries: int = DEFAULT_TOTAL_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    backoff_max: float = DEFAULT_BACKOFF_MAX_SECONDS,
    status_forcelist=DEFAULT_STATUS_FORCELIST,
    allowed_methods=DEFAULT_ALLOWED_METHODS,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> requests.Session:
    """
    Build a requests.Session that automatically retries transient failures
    (connection/read errors, and the given retryable HTTP status codes) with
    exponential backoff.

    Falls back to `timeout` for any call that doesn't pass its own
    timeout=... explicitly.
    """
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        backoff_max=backoff_max,
        status_forcelist=set(status_forcelist),
        allowed_methods=set(allowed_methods),
        raise_on_status=False,
    )
    adapter = _DefaultTimeoutHTTPAdapter(max_retries=retry, default_timeout=timeout)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

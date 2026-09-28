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
# Includes POST, unlike urllib3's own default (which excludes it): every
# adapter in this codebase only uses POST for idempotent-in-practice
# read/query/report-trigger calls, never mutations.
DEFAULT_ALLOWED_METHODS = frozenset(
    {"GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE", "POST"}
)


def build_retrying_session(
    total_retries: int = DEFAULT_TOTAL_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    backoff_max: float = DEFAULT_BACKOFF_MAX_SECONDS,
    status_forcelist=DEFAULT_STATUS_FORCELIST,
    allowed_methods=DEFAULT_ALLOWED_METHODS,
) -> requests.Session:
    """
    Build a requests.Session that automatically retries transient failures
    (connection/read errors, and the given retryable HTTP status codes) with
    exponential backoff.

    Does not set a default timeout - requests never applies one implicitly,
    so every call site must keep passing timeout=... explicitly.
    """
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        backoff_max=backoff_max,
        status_forcelist=set(status_forcelist),
        allowed_methods=set(allowed_methods),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

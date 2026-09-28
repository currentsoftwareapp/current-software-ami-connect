import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from amiadapters.utils.http import build_retrying_session
from test.base_test_case import BaseTestCase


class TestBuildRetryingSession(BaseTestCase):

    def test_default_policy(self):
        session = build_retrying_session()
        adapter = session.get_adapter("https://")
        retry = adapter.max_retries

        self.assertEqual(retry.total, 5)
        self.assertEqual(retry.backoff_factor, 2)
        self.assertEqual(retry.backoff_max, 60)
        self.assertEqual(retry.status_forcelist, {429, 500, 502, 503, 504})
        self.assertEqual(
            retry.allowed_methods,
            {"GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE", "POST"},
        )
        self.assertFalse(retry.raise_on_status)
        self.assertEqual(adapter.default_timeout, 300)

    def test_overrides_take_effect(self):
        session = build_retrying_session(
            total_retries=1,
            backoff_factor=0.5,
            backoff_max=10,
            status_forcelist=(503,),
            allowed_methods=("GET",),
            timeout=10,
        )
        adapter = session.get_adapter("https://")
        retry = adapter.max_retries

        self.assertEqual(retry.total, 1)
        self.assertEqual(retry.backoff_factor, 0.5)
        self.assertEqual(retry.backoff_max, 10)
        self.assertEqual(retry.status_forcelist, {503})
        self.assertEqual(retry.allowed_methods, {"GET"})
        self.assertEqual(adapter.default_timeout, 10)

    def test_retries_transient_failure_then_succeeds(self):
        failures_remaining = {"count": 2}

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if failures_remaining["count"] > 0:
                    failures_remaining["count"] -= 1
                    self.send_response(502)
                    self.end_headers()
                else:
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"ok")

            def log_message(self, format, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.daemon = True
        thread.start()

        try:
            session = build_retrying_session(backoff_factor=0)
            response = session.get(f"http://127.0.0.1:{server.server_port}/", timeout=5)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.text, "ok")
            self.assertEqual(failures_remaining["count"], 0)
        finally:
            server.shutdown()
            thread.join()

    def test_applies_default_timeout_when_caller_omits_one(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")

            def log_message(self, format, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.daemon = True
        thread.start()

        try:
            session = build_retrying_session(timeout=5)
            # No timeout= passed here - the adapter's default_timeout should
            # be applied under the hood rather than blocking forever.
            response = session.get(f"http://127.0.0.1:{server.server_port}/")
            self.assertEqual(response.status_code, 200)
        finally:
            server.shutdown()
            thread.join()

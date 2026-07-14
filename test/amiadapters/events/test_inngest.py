from unittest.mock import MagicMock

from amiadapters.events.inngest import InngestEventPublisher
from test.base_test_case import BaseTestCase


class TestInngestEventPublisher(BaseTestCase):
    def setUp(self):
        self.session_mock = MagicMock()
        self.publisher = InngestEventPublisher(
            event_api_url="https://inngest.example.com",
            event_key="test-event-key",
            session=self.session_mock,
        )

    def test_publish_meter_alert_notify_event_success(self):
        self.publisher.publish_meter_alert_notify_event(organization_id="test_org")
        self.session_mock.post.assert_called_once_with(
            "https://inngest.example.com/e/test-event-key",
            json={
                "name": "meter-alerts/notify/task",
                "data": {"snowflakeId": "test_org"},
            },
            timeout=30,
        )
        self.session_mock.post.return_value.raise_for_status.assert_called_once()

    def test_trailing_slash_in_url_is_normalized(self):
        publisher = InngestEventPublisher(
            event_api_url="https://inngest.example.com/",
            event_key="test-event-key",
            session=self.session_mock,
        )
        publisher.publish_meter_alert_notify_event(organization_id="test_org")
        self.assertEqual(
            self.session_mock.post.call_args.args[0],
            "https://inngest.example.com/e/test-event-key",
        )

    def test_missing_config_raises(self):
        with self.assertRaises(ValueError):
            InngestEventPublisher(event_api_url=None, event_key="k")
        with self.assertRaises(ValueError):
            InngestEventPublisher(event_api_url="https://x", event_key=None)

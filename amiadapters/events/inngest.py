from dataclasses import dataclass
import logging

import requests

logger = logging.getLogger(__name__)


# Matches the event name that triggers the meter-alerts-notify function in the
# Utility Billing app (RegisteredTask.MeterAlertNotify).
METER_ALERT_NOTIFY_EVENT_NAME = "meter-alerts/notify/task"


@dataclass
class MeterAlertNotifyEvent:
    """
    Event that triggers the meter-alerts-notify Inngest function in the Utility
    Billing app. That function pulls the org's current meter alerts from Snowflake
    and notifies the ratepayers tied to each alerting meter.
    """

    org_id: str

    def to_payload(self) -> dict:
        return {
            "name": METER_ALERT_NOTIFY_EVENT_NAME,
            "data": {
                "snowflakeId": self.org_id,
            },
        }


class InngestEventPublisher:
    """
    Publishes events based on AMI Connect adapter activity to Inngest, which runs
    functions in a separate application (the Utility Billing app).
    """

    def __init__(self, event_api_url: str, event_key: str, session=None):
        if not event_api_url:
            raise ValueError("InngestEventPublisher requires an event API URL")
        if not event_key:
            raise ValueError("InngestEventPublisher requires an event key")
        self.event_api_url = event_api_url
        self.event_key = event_key
        self.session = session if session is not None else requests.Session()

    def publish_meter_alert_notify_event(self, organization_id: str):
        event = MeterAlertNotifyEvent(org_id=organization_id)
        url = f"{self.event_api_url.rstrip('/')}/e/{self.event_key}"
        response = self.session.post(url, json=event.to_payload(), timeout=30)
        response.raise_for_status()
        logger.info(
            f"Published meter alert notify event to Inngest for org {organization_id}"
        )

import datetime
import json
from unittest.mock import MagicMock, patch

import pytz
import requests

from amiadapters.configuration.models import (
    LocalIntermediateOutputControllerConfiguration,
)
from amiadapters.adapters.neptune import Consumption, Endpoint, NeptuneAdapter, Reading
from amiadapters.models import DataclassJSONEncoder
from amiadapters.outputs.base import ExtractOutput
from test.base_test_case import BaseTestCase


class TestMetersenseAdapter(BaseTestCase):

    def setUp(self):
        self.adapter = NeptuneAdapter(
            org_id="test-org",
            org_timezone=pytz.UTC,
            pipeline_configuration=self.TEST_PIPELINE_CONFIGURATION,
            site_id="1001",
            api_key="api-key",
            client_id="client-id",
            client_secret="client-secret",
            configured_task_output_controller=LocalIntermediateOutputControllerConfiguration(
                "/tmp/output"
            ),
            configured_meter_alerts=self.TEST_METER_ALERT_CONFIGURATION,
            configured_metrics=self.TEST_METRICS_CONFIGURATION,
            configured_sinks=None,
        )

    def _mock_response(self, json_data, status=200):
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.json.return_value = json_data
        mock_resp.status_code = status
        mock_resp.raise_for_status = MagicMock()
        return mock_resp

    def _set_up_mock_api_calls(self, mock_get):
        # Define fake API responses for each endpoint called during _extract
        # The adapter calls _get_endpoints, _get_consumption, _get_readings
        # Each calls requests.get with different URLs/params

        # Prepare paged responses for /api/v2/endpoints
        endpoints_response = {
            "endpoints": [
                {
                    "site_id": "1001",
                    "account_number": "AC1",
                    "premise_key": "PK1",
                    "miu_id": "MIU1",
                    "register_id": "REG1",
                    "meter_number": "MTR1",
                    "meter_type": "Water",
                    "meter_size": "1.0",
                    "meter_manufacturer": "Neptune",
                    "dials": "4",
                    "multiplier": "1",
                    "unit_of_measure": "CF",
                    "cycle_route": "R1",
                }
            ],
            "paging": {"next": None},
        }

        # Prepare paged responses for /api/v1/consumption
        consumption_response = {
            "site_id": "1001",
            "endpoints": [
                {
                    "meter_number": "MTR1",
                    "miu_id": "MIU1",
                    "consumption_history": [
                        {
                            "reading_date": "2021-01-01 00:00:00",
                            "consumption": "10",
                            "consumption_with_multiplier": "10",
                        }
                    ],
                }
            ],
            "paging": {"next": None},
        }

        # Prepare paged responses for /api/v2/readings
        readings_response = {
            "site_id": "1001",
            "endpoints": [
                {
                    "meter_number": "MTR1",
                    "miu_id": "MIU1",
                    "readings": [
                        {
                            "reading": "100",
                            "reading_datetime": "2021-01-01 00:00:00",
                            "consumption_state": "0",
                            "consumption_35_days": "0",
                            "reverse_flow_state": "0",
                            "no_flow_35_days": "0",
                            "register_change_out": "0",
                        }
                    ],
                }
            ],
            "paging": {"next": None},
        }

        # We need to respond with the correct JSON depending on the URL called.
        def side_effect(url, headers=None, params=None):
            if url.endswith("/api/v2/endpoints"):
                return self._mock_response(endpoints_response)
            elif url.endswith("/api/v1/consumption"):
                return self._mock_response(consumption_response)
            elif url.endswith("/api/v2/readings"):
                return self._mock_response(readings_response)
            elif url.endswith("/api/v1/token"):
                # Token response
                token_response = {"AccessToken": "fake-token"}
                return self._mock_response(token_response)
            else:
                # Default fallback for any other request
                return self._mock_response({"paging": {"next": None}, "endpoints": []})

        mock_get.side_effect = side_effect
        return mock_get, endpoints_response, consumption_response, readings_response

    @patch("requests.get")
    def test_extract_with_mocked_requests(self, mock_get):
        mock_get, endpoints_response, consumption_response, readings_response = (
            self._set_up_mock_api_calls(mock_get)
        )

        output = self.adapter._extract(
            run_id="run1",
            extract_range_start=datetime.datetime(2021, 1, 1),
            extract_range_end=datetime.datetime(2021, 1, 2),
        )

        # Once for token, then once for each url
        self.assertEqual(4, mock_get.call_count)

        # Check the output keys
        self.assertIn("endpoints.json", output.outputs)
        self.assertIn("consumption.json", output.outputs)
        self.assertIn("readings.json", output.outputs)

        # Parse the first JSON line of endpoints
        endpoints_lines = output.outputs["endpoints.json"].split("\n")
        endpoint_obj = json.loads(endpoints_lines[0])
        self.assertEqual(endpoint_obj["meter_number"], "MTR1")

        # Parse the first JSON line of consumption
        consumption_lines = output.outputs["consumption.json"].split("\n")
        consumption_obj = json.loads(consumption_lines[0])
        self.assertEqual(consumption_obj["consumption_amount"], "10")

        # Parse the first JSON line of readings
        readings_lines = output.outputs["readings.json"].split("\n")
        reading_obj = json.loads(readings_lines[0])
        self.assertEqual(reading_obj["reading"], "100")

    def test_transform(self):
        endpoint = Endpoint(
            site_id="1001",
            account_number="1234-56-7",
            premise_key="PK1",
            miu_id="MIU1",
            register_id="REG1",
            meter_number="MTR1",
            meter_type="Water",
            meter_size="1.0",
            meter_manufacturer="Neptune",
            dials="4",
            multiplier="2",
            unit_of_measure="CF",
            cycle_route="R1",
        )

        consumption = Consumption(
            meter_number="MTR1",
            miu_id="MIU1",
            consumption_amount="1000",
            consumption_with_multiplier="10",
            reading_date_utc="2021-01-01T00:00:00",
        )

        reading = Reading(
            meter_number="MTR1",
            miu_id="MIU1",
            site_id="1001",
            reading="123456",
            reading_datetime_utc="2021-01-01T00:00:00",
            consumption_state="0",
            consumption_35_days="0",
            reverse_flow_state="0",
            no_flow_35_days="0",
            register_change_out="0",
        )

        extract_outputs = ExtractOutput(
            {
                "endpoints.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in [endpoint]
                ),
                "consumption.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in [consumption]
                ),
                "readings.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in [reading]
                ),
            }
        )

        meters, reads = self.adapter._transform(
            run_id="run1", extract_outputs=extract_outputs
        )

        # meters should contain one GeneralMeter with device_id "MTR1"
        self.assertEqual(len(meters), 1)
        self.assertEqual(meters[0].device_id, "MTR1-MIU1")
        self.assertEqual(meters[0].location_id, "1234-56")
        self.assertEqual(meters[0].multiplier, "2")
        self.assertEqual(meters[0].meter_manufacturer, "Neptune")

        # reads should contain one GeneralMeterRead combining interval and register data
        self.assertEqual(len(reads), 1)
        read = reads[0]
        self.assertEqual(read.device_id, "MTR1-MIU1")
        self.assertEqual(read.location_id, "1234-56")
        self.assertEqual(read.flowtime.isoformat(), "2021-01-01T00:00:00+00:00")
        # interval_value is consumption_with_multiplier from consumption (10)
        self.assertEqual(read.interval_value, 10.0)
        # register_value is the register count times the multiplier taken from consumption (123456 * 0.01)
        self.assertEqual(read.register_value, 1234.56)

    def test_transform_register_without_consumption(self):
        endpoint = Endpoint(
            site_id="1001",
            account_number="1234-56-7",
            premise_key="PK1",
            miu_id="MIU1",
            register_id="REG1",
            meter_number="MTR1",
            meter_type="Water",
            meter_size="1.0",
            meter_manufacturer="Neptune",
            dials="4",
            multiplier=None,
            unit_of_measure="CF",
            cycle_route="R1",
        )
        reading = Reading(
            meter_number="MTR1",
            miu_id="MIU1",
            site_id="1001",
            reading="123456",
            reading_datetime_utc="2021-01-01T00:00:00",
            consumption_state="0",
            consumption_35_days="0",
            reverse_flow_state="0",
            no_flow_35_days="0",
            register_change_out="0",
        )
        extract_outputs = ExtractOutput(
            {
                "endpoints.json": json.dumps(endpoint, cls=DataclassJSONEncoder),
                "consumption.json": "",
                "readings.json": json.dumps(reading, cls=DataclassJSONEncoder),
            }
        )

        meters, reads = self.adapter._transform(
            run_id="run1", extract_outputs=extract_outputs
        )

        # Without consumption there is no multiplier, so the raw register count is not stored
        self.assertEqual(len(reads), 1)
        self.assertIsNone(reads[0].register_value)
        self.assertIsNone(reads[0].interval_value)

    def test_register_multiplier(self):
        def consumption(amount, with_multiplier):
            return Consumption(
                meter_number="MTR1",
                miu_id="MIU1",
                consumption_amount=amount,
                consumption_with_multiplier=with_multiplier,
                reading_date_utc="2021-01-01T00:00:00",
            )

        # Neptune rounds consumption_with_multiplier to three decimals
        self.assertEqual(
            self.adapter._register_multiplier([consumption("367.866", "36.787")]), 0.1
        )
        self.assertEqual(
            self.adapter._register_multiplier([consumption("-235.784", "-2.358")]), 0.01
        )
        self.assertEqual(
            self.adapter._register_multiplier([consumption("9.251", "9.251")]), 1
        )
        # Zeros and unparseable values are skipped; the most common multiplier wins
        self.assertEqual(
            self.adapter._register_multiplier(
                [
                    consumption("0", "0"),
                    consumption("0.032", "0.0"),
                    consumption(None, "1"),
                    consumption("57", "0.57"),
                    consumption("18", "0.18"),
                    consumption("18", "1.8"),
                ]
            ),
            0.01,
        )
        self.assertIsNone(self.adapter._register_multiplier([consumption("0", "0")]))
        self.assertIsNone(self.adapter._register_multiplier([]))

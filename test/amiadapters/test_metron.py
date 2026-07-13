import datetime
import pytz
from unittest import mock

from amiadapters.adapters.metron import (
    MetronAdapter,
    MetronReading,
)
from amiadapters.models import GeneralMeter, GeneralMeterRead

from test.base_test_case import BaseTestCase, MockResponse, mocked_response_500


def mocked_get_billing(*args, **kwargs):
    data = [
        {
            "Meter_ID": "3003008",
            "LCD_Read": "10357889",
            "Billing_Read": "10357889",
            "Read_Date": "05/15/2018",
            "Unit": "G",
            "Reference": "2600",
            "Account_Name": "Jack",
            "Address": "123 Main St",
            "Utility_Defined": "Custom Note",
        }
    ]
    return MockResponse(data, 200)


class TestMetronAdapter(BaseTestCase):

    def setUp(self):
        self.adapter = MetronAdapter(
            org_id="this-utility",
            org_timezone=pytz.UTC,
            pipeline_configuration=self.TEST_PIPELINE_CONFIGURATION,
            username="user",
            password="pass",
            configured_task_output_controller=self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
            configured_meter_alerts=self.TEST_METER_ALERT_CONFIGURATION,
            configured_metrics=self.TEST_METRICS_CONFIGURATION,
            configured_sinks=[],
        )
        self.range_start = datetime.datetime(2026, 1, 25, 0, 0)
        self.range_end = datetime.datetime(2026, 1, 26, 0, 0)

    def test_init(self):
        self.assertEqual("this-utility", self.adapter.org_id)
        self.assertEqual("metron-this-utility", self.adapter.name())

    @mock.patch("requests.get", side_effect=[mocked_get_billing()])
    def test_extract_reads(self, mock_get):
        result = self.adapter._extract_reads(self.range_start, self.range_end)
        self.assertEqual(1, len(result))
        self.assertEqual(
            MetronReading(
                meter_id="3003008",
                lcd_read="10357889",
                billing_read="10357889",
                read_date="05/15/2018",
                unit="G",
                reference="2600",
                account_name="Jack",
                address="123 Main St",
                utility_defined="Custom Note",
            ),
            result[0],
        )
        call = mock_get.call_args_list[0]
        self.assertEqual("https://webapi.waterscope.us/api/Billing", call.args[0])
        params = call.kwargs["params"]
        self.assertEqual("user", params["username"])
        self.assertEqual("pass", params["password"])
        # billingDate is the end of the range, formatted MM/DD/YYYY.
        self.assertEqual("01/26/2026", params["billingDate"])
        # numberDaysWindow is the span of the extract range in days.
        self.assertEqual(1, params["numberDaysWindow"])

    @mock.patch("requests.get", side_effect=[mocked_get_billing()])
    def test_extract_reads_window_spans_range(self, mock_get):
        wide_start = datetime.datetime(2026, 1, 1, 0, 0)
        wide_end = datetime.datetime(2026, 1, 31, 0, 0)
        self.adapter._extract_reads(wide_start, wide_end)
        params = mock_get.call_args_list[0].kwargs["params"]
        self.assertEqual(30, params["numberDaysWindow"])

    @mock.patch("requests.get", side_effect=[mocked_response_500()])
    def test_extract__non_200_raises(self, mock_get):
        with self.assertRaises(Exception):
            self.adapter._extract_reads(self.range_start, self.range_end)

    def test_transform_meters_and_reads(self):
        reads = [
            MetronReading(
                meter_id="3003008",
                lcd_read="10357889",
                billing_read="10357889",
                read_date="05/15/2018",
                unit="G",
                reference="2600",
                account_name="Jack",
                address="123 Main St",
                utility_defined="Custom Note",
            )
        ]

        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(reads)
        )

        self.assertEqual(
            [
                GeneralMeter(
                    org_id="this-utility",
                    device_id="3003008",
                    account_id="2600",
                    location_id="2600",
                    meter_id="3003008",
                    endpoint_id=None,
                    meter_install_date=None,
                    meter_size=None,
                    meter_manufacturer="Metron",
                    multiplier=None,
                    location_address="123 Main St",
                    location_city=None,
                    location_state=None,
                    location_zip=None,
                )
            ],
            transformed_meters,
        )

        # 10357889 gallons converted to cubic feet.
        expected_value, expected_unit = self.adapter.map_reading(10357889.0, "GAL")
        self.assertEqual(
            [
                GeneralMeterRead(
                    org_id="this-utility",
                    device_id="3003008",
                    account_id="2600",
                    location_id="2600",
                    flowtime=self.adapter.org_timezone.localize(
                        datetime.datetime(2018, 5, 15, 0, 0)
                    ),
                    register_value=expected_value,
                    register_unit=expected_unit,
                    interval_value=None,
                    interval_unit=None,
                    battery=None,
                    install_date=None,
                    connection=None,
                    estimated=None,
                )
            ],
            transformed_reads,
        )

    def test_transform_dedupes_meter_across_reads(self):
        reads = [
            MetronReading(
                meter_id="3003008",
                lcd_read="100",
                billing_read="100",
                read_date="05/15/2018",
                unit="G",
                reference="2600",
                account_name="Jack",
                address="123 Main St",
                utility_defined=None,
            ),
            MetronReading(
                meter_id="3003008",
                lcd_read="200",
                billing_read="200",
                read_date="05/16/2018",
                unit="G",
                reference="2600",
                account_name="Jack",
                address="123 Main St",
                utility_defined=None,
            ),
        ]

        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(reads)
        )

        self.assertEqual(1, len(transformed_meters))
        self.assertEqual(2, len(transformed_reads))

    def test_transform_falls_back_to_lcd_read(self):
        reads = [
            MetronReading(
                meter_id="3003008",
                lcd_read="4242",
                billing_read=None,
                read_date="05/15/2018",
                unit="G",
                reference="2600",
                account_name="Jack",
                address="123 Main St",
                utility_defined=None,
            )
        ]

        _, transformed_reads = self.adapter._transform_meters_and_reads(reads)

        expected_value, _ = self.adapter.map_reading(4242.0, "GAL")
        self.assertEqual(1, len(transformed_reads))
        self.assertEqual(expected_value, transformed_reads[0].register_value)

    def test_transform_unknown_unit_keeps_value_unconverted(self):
        reads = [
            MetronReading(
                meter_id="3003008",
                lcd_read="500",
                billing_read="500",
                read_date="05/15/2018",
                unit="ZZZ",
                reference="2600",
                account_name="Jack",
                address="123 Main St",
                utility_defined=None,
            )
        ]

        _, transformed_reads = self.adapter._transform_meters_and_reads(reads)

        # Unknown unit -> value kept as-is with no unit rather than a wrong conversion.
        self.assertEqual(1, len(transformed_reads))
        self.assertEqual(500.0, transformed_reads[0].register_value)
        self.assertIsNone(transformed_reads[0].register_unit)

    def test_transform_skips_read_missing_meter_id(self):
        reads = [
            MetronReading(
                meter_id=None,
                lcd_read="1",
                billing_read="1",
                read_date="05/15/2018",
                unit="G",
                reference="2600",
                account_name="Jack",
                address="123 Main St",
                utility_defined=None,
            ),
            MetronReading(
                meter_id="3003008",
                lcd_read="2",
                billing_read="2",
                read_date="05/15/2018",
                unit="G",
                reference="2600",
                account_name="Jack",
                address="123 Main St",
                utility_defined=None,
            ),
        ]

        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(reads)
        )

        self.assertEqual(1, len(transformed_meters))
        self.assertEqual(1, len(transformed_reads))
        self.assertEqual("3003008", transformed_reads[0].device_id)

    def test_transform_skips_read_missing_read_date(self):
        reads = [
            MetronReading(
                meter_id="3003008",
                lcd_read="1",
                billing_read="1",
                read_date=None,
                unit="G",
                reference="2600",
                account_name="Jack",
                address="123 Main St",
                utility_defined=None,
            )
        ]

        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(reads)
        )

        # The meter is still built, but the undated read is skipped.
        self.assertEqual(1, len(transformed_meters))
        self.assertEqual(0, len(transformed_reads))

    def test_transform_meter_alerts_is_empty(self):
        self.assertEqual([], self.adapter._transform_meter_alerts("run", None))

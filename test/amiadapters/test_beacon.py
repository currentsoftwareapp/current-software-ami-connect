import datetime
import json
import pytz
from unittest import mock

from amiadapters.adapters.beacon import (
    Beacon360Adapter,
    Beacon360Exception,
    Beacon360LeakAlert,
    Beacon360MeterAndRead,
    BeaconExceptionsRawTableLoader,
    BeaconLeaksRawTableLoader,
    BeaconRawTableLoader,
    BeaconReportClient,
    BEACON_RAW_SNOWFLAKE_LOADER,
    REQUESTED_COLUMNS_FOR_READS,
    STALE_OPEN_TAMPER_THRESHOLD,
)
from amiadapters.models import (
    DataclassJSONEncoder,
    GeneralMeter,
    GeneralMeterAlert,
    GeneralMeterRead,
)
from amiadapters.outputs.base import ExtractOutput

from test.base_test_case import (
    BaseTestCase,
    MockResponse,
    mocked_response_429,
    mocked_response_500,
)


def mocked_create_range_report(*args, **kwargs):
    data = {
        "edsUUID": "acecd48e8b794f49a61c2b96c9ff9118",
        "statusUrl": "/v2/eds/status/acecd48e8b794f49a61c2b96c9ff9118",
    }
    return MockResponse(data, 202)


def mocked_get_range_report_status_not_finished(*args, **kwargs):
    data = {
        "message": "acecd48e8b794f49a61c2b96c9ff9118 operation running.",
        "progress": {"percentComplete": 5.0},
        "queueTime": "2025-03-18T18:52:41Z",
        "state": "run",
    }
    return MockResponse(data, 200)


def mocked_get_range_report_status_finished(*args, **kwargs):
    data = {
        "endTime": "2025-03-18T18:56:14Z",
        "lastMeterId": "800380.1",
        "message": "acecd48e8b794f49a61c2b96c9ff9118 operation succeeded",
        "queueTime": "2025-03-18T18:52:41Z",
        "reportUrl": "/v1/content/8021910600018033071/users/4928669927440453458/export90868",
        "state": "done",
    }
    return MockResponse(data, 200)


def mocked_get_report_from_link(text, *args, **kwargs):
    return MockResponse(None, 200, text=text)


def mocked_exception_from_status_check(*args, **kwargs):
    data = {"state": "exception"}
    return MockResponse(data, 200)


def mocked_get_consumption_response_last_page(*args, **kwargs):
    data = {"meters": [], "currentPage": 2, "itemsOnPage": 0, "totalCount": 1}
    return MockResponse(data, 200)


def beacon_meter_and_read_factory(
    account_id: str = "303022",
    meter_id: str = "1470158170",
    endpoint_id: str = "22",
    flow_time: str = "2024-08-01 00:59",
    meter_install_date: str = "",
) -> Beacon360MeterAndRead:
    return Beacon360MeterAndRead(
        Account_ID=account_id,
        Backflow_Gallons="",
        Battery_Level="good",
        Endpoint_SN=endpoint_id,
        Estimated_Flag="0",
        Flow="5.0",
        Flow_Time=flow_time,
        Flow_Unit="Gallons",
        Location_Address_Line1="5391 E. MYSTREET",
        Location_Address_Line2="",
        Location_Address_Line3="",
        Location_City="Apple",
        Location_Country="US",
        Location_ID=account_id,
        Location_State="CA",
        Location_ZIP="93727",
        Meter_ID=meter_id,
        Meter_Install_Date=meter_install_date,
        Meter_Manufacturer="BADGER",
        Meter_Model="T-10",
        Meter_Size="0.625",
        Meter_Size_Desc='5/8"',
        Meter_Size_Unit="INCHES",
        Meter_SN="",
        Raw_Read="022760",
        Read="227.6",
        Read_Time=flow_time,
        Read_Unit="CCF",
        Current_Leak_Rate="",
        Current_Leak_Start_Date="",
        Demand_Zone_ID="",
        Dials="",
        Endpoint_Install_Date="2024-08-01 00:59",
        Location_Continuous_Flow="",
        Location_Latitude="",
        Location_Longitude="",
        Location_Irrigated_Area="",
        Location_Irrigation="",
        Location_Main_Use="",
        Location_Name="",
        Location_Pool="",
        Location_Water_Type="",
        Location_Year_Built="",
        Register_Number="",
        Register_Resolution="",
        SA_Start_Date="",
        Service_Point_Class_Code="",
        Service_Point_Class_Code_Normalized="",
        Signal_Strength="marginal",
    )


class TestBeacon360Adapter(BaseTestCase):

    report_csv = BaseTestCase.load_fixture("beacon-360-report.csv")

    def setUp(self):
        self.maxDiff = None
        self.adapter = Beacon360Adapter(
            api_user="user",
            api_password="pass",
            pipeline_configuration=self.TEST_PIPELINE_CONFIGURATION,
            use_cache=False,
            org_id="test-org",
            org_timezone=pytz.timezone("America/Los_Angeles"),
            configured_task_output_controller=self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
            configured_meter_alerts=self.TEST_METER_ALERT_CONFIGURATION,
            configured_metrics=self.TEST_METRICS_CONFIGURATION,
            configured_sinks=[],
            cache_output_folder="/tmp/output",
        )
        self.range_start = datetime.datetime(2024, 1, 2, 0, 0)
        self.range_end = datetime.datetime(2024, 1, 3, 0, 0)

    def test_init(self):
        self.assertEqual("/tmp/output", self.adapter.output_controller.output_folder)
        self.assertEqual("user", self.adapter.user)
        self.assertEqual("pass", self.adapter.password)
        self.assertEqual("beacon-360-test-org", self.adapter.name())
        self.assertEqual(3, len(self.adapter.scheduled_extracts()))

    def test_scheduled_extracts__scheduled_with_lag(self):
        result = self.adapter.scheduled_extracts()
        self.assertEqual(3, len(result))

        standard_extract = result[0]
        self.assertEqual("standard", standard_extract.name)
        self.assertEqual(datetime.timedelta(days=2), standard_extract.interval)
        self.assertEqual(datetime.timedelta(days=0), standard_extract.lag)
        self.assertEqual("0 12 * * *", standard_extract.schedule_crontab)

        lagged_extract = result[1]
        self.assertEqual("lagged", lagged_extract.name)
        self.assertEqual(datetime.timedelta(days=1), lagged_extract.interval)
        self.assertEqual(datetime.timedelta(days=14), lagged_extract.lag)
        self.assertEqual("0 10 * * *", lagged_extract.schedule_crontab)

        lagged_extract = result[2]
        self.assertEqual("half-year-lagged", lagged_extract.name)
        self.assertEqual(datetime.timedelta(days=1), lagged_extract.interval)
        self.assertEqual(datetime.timedelta(days=6 * 30), lagged_extract.lag)
        self.assertEqual("0 11 * * *", lagged_extract.schedule_crontab)

    @mock.patch("requests.get")
    @mock.patch("requests.post")
    def test_fetch_range_report__uses_cache(self, mock_post, mock_get):
        self.adapter.use_cache = True
        self.adapter._get_cached_report = mock.MagicMock(return_value=self.report_csv)

        result = self.adapter._fetch_range_report(self.range_start, self.range_end)
        self.assertEqual(self.report_csv, result)
        self.assertEqual(0, mock_get.call_count)
        self.assertEqual(0, mock_post.call_count)

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_range_report_status_not_finished(),
            mocked_get_range_report_status_finished(),
            mocked_get_report_from_link(text=report_csv),
        ],
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__can_fetch_report_from_api(
        self, mock_sleep, mock_post, mock_get
    ):
        result = self.adapter._fetch_range_report(self.range_start, self.range_end)
        self.assertEqual(self.report_csv, result)

        self.assertEqual(1, len(mock_post.call_args_list))
        generate_report_request = mock_post.call_args_list[0]
        self.assertEqual(
            "https://api.beaconama.net/v2/eds/range",
            generate_report_request.kwargs["url"],
        )
        self.assertEqual(
            ",".join(REQUESTED_COLUMNS_FOR_READS),
            generate_report_request.kwargs["params"]["Header_Columns"],
        )
        self.assertEqual(
            self.range_start,
            generate_report_request.kwargs["params"]["Start_Date"],
        )
        self.assertEqual(
            self.range_end,
            generate_report_request.kwargs["params"]["End_Date"],
        )
        self.assertTrue(generate_report_request.kwargs["params"]["Has_Endpoint"])
        self.assertEqual(
            "hourly", generate_report_request.kwargs["params"]["Resolution"]
        )
        self.assertEqual(
            {"Content-Type": "application/x-www-form-urlencoded"},
            generate_report_request.kwargs["headers"],
        )

        self.assertEqual(3, len(mock_get.call_args_list))
        self.assertIn(
            "https://api.beaconama.net/v2/eds/status/",
            mock_get.call_args_list[0].kwargs["url"],
        )
        self.assertIn(
            "https://api.beaconama.net/v2/eds/status/",
            mock_get.call_args_list[1].kwargs["url"],
        )
        self.assertIn(
            "https://api.beaconama.net/v1/content/",
            mock_get.call_args_list[2].kwargs["url"],
        )

        self.assertEqual(1, mock_sleep.call_count)

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_range_report_status_not_finished(),
            mocked_get_range_report_status_finished(),
            mocked_get_report_from_link(text=report_csv),
        ],
    )
    @mock.patch("requests.post", side_effect=[mocked_response_429()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__throws_exception_when_rate_limit_exceeded_when_report_generated(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report(self.range_start, self.range_end)

        self.assertTrue("Rate limit exceeded" in str(context.exception))

    @mock.patch("requests.get", side_effect=[])
    @mock.patch("requests.post", side_effect=[mocked_response_500()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__throws_exception_when_non_202_from_report_generation(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report(self.range_start, self.range_end)

        self.assertTrue("Failed request to generate report" in str(context.exception))

    @mock.patch("requests.get", side_effect=[mocked_response_500()])
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__throws_exception_when_status_response_non_200(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report(self.range_start, self.range_end)

        self.assertTrue("Failed request to get report status" in str(context.exception))

    @mock.patch("requests.get", side_effect=[mocked_exception_from_status_check()])
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__throws_exception_when_status_response_indicates_exception(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report(self.range_start, self.range_end)

        self.assertTrue("Exception found in report status" in str(context.exception))

    # Mock the status call response as "not finished" way more times than our max limit
    @mock.patch(
        "requests.get",
        side_effect=[mocked_get_range_report_status_not_finished()] * 500,
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__throws_exception_when_max_attempts_reached_while_polling_for_status(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report(self.range_start, self.range_end)

        self.assertTrue("Reached max attempts" in str(context.exception))

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_range_report_status_finished(),
            Exception,
            mocked_get_report_from_link(text=report_csv),
        ],
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__retries_once_when_fetch_report_throws_exception(
        self, mock_sleep, mock_post, mock_get
    ):
        result = self.adapter._fetch_range_report(self.range_start, self.range_end)
        self.assertEqual(self.report_csv, result)
        self.assertEqual(1, mock_sleep.call_count)

    @mock.patch(
        "requests.get",
        side_effect=[mocked_get_range_report_status_finished(), mocked_response_500()],
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__throws_exception_when_fetch_report_returns_non_200(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report(self.range_start, self.range_end)

        self.assertTrue("Failed request to download report" in str(context.exception))

    def test_report_to_output(self):
        result = self.adapter._report_to_output_stream(self.report_csv)
        result = [r for r in result]
        result = [Beacon360MeterAndRead(**json.loads(d)) for d in result]

        expected = [
            beacon_meter_and_read_factory(
                flow_time="2024-08-01 00:59", endpoint_id="130615549"
            ),
            beacon_meter_and_read_factory(
                flow_time="2024-08-01 01:59", endpoint_id="130615549"
            ),
        ]

        self.assertEqual(expected, result)

    def test_leaks_report_to_output(self):
        leaks_csv = (
            "Account_ID,Endpoint_SN,Current_Leak_Start_Date,Current_Leak_Rate,Current_Leak_Unit\n"
            "303022,1470158170,2024-08-01 00:00,1.5,Gallons Per Hour\n"
            "303023,1470158171,2024-08-02 00:00,2.0,Gallons Per Hour\n"
        )
        result = list(self.adapter._leaks_report_to_output_stream(leaks_csv))
        result = [Beacon360LeakAlert(**json.loads(d)) for d in result]

        expected = [
            Beacon360LeakAlert(
                Account_ID="303022",
                Endpoint_SN="1470158170",
                Current_Leak_Start_Date="2024-08-01 00:00",
                Current_Leak_Rate="1.5",
                Current_Leak_Unit="Gallons Per Hour",
            ),
            Beacon360LeakAlert(
                Account_ID="303023",
                Endpoint_SN="1470158171",
                Current_Leak_Start_Date="2024-08-02 00:00",
                Current_Leak_Rate="2.0",
                Current_Leak_Unit="Gallons Per Hour",
            ),
        ]
        self.assertEqual(expected, result)

    def test_exceptions_report_to_output(self):
        exceptions_csv = (
            "Account_ID,Endpoint_SN,Exception_Start_Date,Exception_End_Date,Exception\n"
            "303022,1470158170,2024-08-01 00:00,2024-08-02 00:00,Tamper\n"
            "303022,1470158170,2024-08-03 00:00,,EncoderAlert\n"
        )
        result = list(self.adapter._exceptions_report_to_output_stream(exceptions_csv))
        result = [Beacon360Exception(**json.loads(d)) for d in result]

        expected = [
            Beacon360Exception(
                Account_ID="303022",
                Endpoint_SN="1470158170",
                Exception_Start_Date="2024-08-01 00:00",
                Exception_End_Date="2024-08-02 00:00",
                Exception="Tamper",
            ),
            Beacon360Exception(
                Account_ID="303022",
                Endpoint_SN="1470158170",
                Exception_Start_Date="2024-08-03 00:00",
                Exception_End_Date="",
                Exception="EncoderAlert",
            ),
        ]
        self.assertEqual(expected, result)

    def test_transform_meter_alerts__exceptions(self):
        exception = Beacon360Exception(
            Account_ID="303022",
            Endpoint_SN="130615549",
            Exception_Start_Date="2024-08-01 00:00",
            Exception_End_Date="2024-08-02 00:00",
            Exception="Tamper",
        )
        extract_outputs = ExtractOutput(
            {"exceptions.json": json.dumps(exception, cls=DataclassJSONEncoder)}
        )
        result = self.adapter._transform_meter_alerts("run-id", extract_outputs)

        self.assertEqual(1, len(result))
        alert = result[0]
        self.assertEqual("test-org", alert.org_id)
        self.assertEqual("130615549", alert.device_id)
        self.assertEqual("Tamper", alert.alert_type)
        self.assertEqual(
            self.adapter.org_timezone.localize(datetime.datetime(2024, 8, 1, 0, 0)),
            alert.start_time,
        )
        self.assertEqual(
            self.adapter.org_timezone.localize(datetime.datetime(2024, 8, 2, 0, 0)),
            alert.end_time,
        )
        self.assertEqual("Beacon 360", alert.source)

    def test_transform_meter_alerts__active_exception_has_no_end_time(self):
        exception = Beacon360Exception(
            Account_ID="303022",
            Endpoint_SN="130615549",
            Exception_Start_Date="2024-08-01 00:00",
            Exception_End_Date="",
            Exception="EncoderAlert",
        )
        extract_outputs = ExtractOutput(
            {"exceptions.json": json.dumps(exception, cls=DataclassJSONEncoder)}
        )
        result = self.adapter._transform_meter_alerts("run-id", extract_outputs)
        self.assertIsNone(result[0].end_time)

    def test_transform_meter_alerts__skips_exception_with_empty_endpoint_sn(self):
        exception = Beacon360Exception(
            Account_ID="303022",
            Endpoint_SN="",
            Exception_Start_Date="2024-08-01 00:00",
            Exception_End_Date="2024-08-02 00:00",
            Exception="Tamper",
        )
        extract_outputs = ExtractOutput(
            {"exceptions.json": json.dumps(exception, cls=DataclassJSONEncoder)}
        )
        result = self.adapter._transform_meter_alerts("run-id", extract_outputs)
        self.assertEqual([], result)

    def test_transform_meter_alerts__stale_open_endpoint_tamper_is_capped(self):
        exception = Beacon360Exception(
            Account_ID="303022",
            Endpoint_SN="130615549",
            Exception_Start_Date="2020-01-01 00:00",
            Exception_End_Date="",
            Exception="Endpoint Tamper",
        )
        extract_outputs = ExtractOutput(
            {"exceptions.json": json.dumps(exception, cls=DataclassJSONEncoder)}
        )
        result = self.adapter._transform_meter_alerts("run-id", extract_outputs)
        self.assertEqual(1, len(result))
        alert = result[0]
        expected_start = self.adapter.org_timezone.localize(
            datetime.datetime(2020, 1, 1, 0, 0)
        )
        self.assertEqual(expected_start, alert.start_time)
        self.assertEqual(expected_start + STALE_OPEN_TAMPER_THRESHOLD, alert.end_time)

    @mock.patch("amiadapters.adapters.beacon.datetime")
    def test_transform_meter_alerts__recent_open_endpoint_tamper_is_not_capped(
        self, mock_dt
    ):
        start = self.adapter.org_timezone.localize(datetime.datetime(2026, 5, 1, 0, 0))
        mock_dt.now.return_value = start + datetime.timedelta(days=10)
        exception = Beacon360Exception(
            Account_ID="303022",
            Endpoint_SN="130615549",
            Exception_Start_Date="2026-05-01 00:00",
            Exception_End_Date="",
            Exception="Endpoint Tamper",
        )
        extract_outputs = ExtractOutput(
            {"exceptions.json": json.dumps(exception, cls=DataclassJSONEncoder)}
        )
        result = self.adapter._transform_meter_alerts("run-id", extract_outputs)
        self.assertEqual(1, len(result))
        self.assertIsNone(result[0].end_time)

    def test_transform_meter_alerts__old_non_tamper_open_exception_is_not_capped(self):
        exception = Beacon360Exception(
            Account_ID="303022",
            Endpoint_SN="130615549",
            Exception_Start_Date="2020-01-01 00:00",
            Exception_End_Date="",
            Exception="EncoderAlert",
        )
        extract_outputs = ExtractOutput(
            {"exceptions.json": json.dumps(exception, cls=DataclassJSONEncoder)}
        )
        result = self.adapter._transform_meter_alerts("run-id", extract_outputs)
        self.assertEqual(1, len(result))
        self.assertIsNone(result[0].end_time)

    def test_transform_meters_and_reads(self):
        raw_meters_with_reads = [
            beacon_meter_and_read_factory(
                flow_time="2024-08-01 00:59", meter_install_date="2016-01-01 23:59"
            ),
            beacon_meter_and_read_factory(
                flow_time="2024-08-01 01:59", meter_install_date="2016-01-01 23:59"
            ),
        ]
        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(raw_meters_with_reads)
        )
        transformed_meters = list(sorted(transformed_meters, key=lambda m: m.meter_id))

        expected_meters = [
            GeneralMeter(
                org_id="test-org",
                device_id="22",
                account_id="303022",
                location_id="303022",
                meter_id="1470158170",
                endpoint_id="22",
                meter_install_date=self.adapter.org_timezone.localize(
                    datetime.datetime(2016, 1, 1, 23, 59)
                ),
                meter_size="0.625",
                meter_manufacturer="BADGER",
                multiplier=None,
                location_address="5391 E. MYSTREET",
                location_city="Apple",
                location_state="CA",
                location_zip="93727",
            )
        ]
        self.assertEqual(
            "2016-01-01T23:59:00-08:00",
            transformed_meters[0].meter_install_date.isoformat(),
        )
        self.assertListEqual(expected_meters, transformed_meters)

        expected_reads = [
            GeneralMeterRead(
                org_id="test-org",
                device_id="22",
                account_id="303022",
                location_id="303022",
                flowtime=self.adapter.org_timezone.localize(
                    datetime.datetime(2024, 8, 1, 0, 59)
                ),
                register_value=22760.0,
                register_unit="CF",
                interval_value=0.66840273,
                interval_unit="CF",
                battery="good",
                install_date=self.adapter.org_timezone.localize(
                    datetime.datetime(2024, 8, 1, 0, 59)
                ),
                connection="marginal",
                estimated=0,
            ),
            GeneralMeterRead(
                org_id="test-org",
                device_id="22",
                account_id="303022",
                location_id="303022",
                flowtime=self.adapter.org_timezone.localize(
                    datetime.datetime(2024, 8, 1, 1, 59)
                ),
                register_value=22760.0,
                register_unit="CF",
                interval_value=0.66840273,
                interval_unit="CF",
                battery="good",
                install_date=self.adapter.org_timezone.localize(
                    datetime.datetime(2024, 8, 1, 0, 59)
                ),
                connection="marginal",
                estimated=0,
            ),
        ]
        self.assertEqual(
            "2024-08-01T00:59:00-07:00", transformed_reads[0].flowtime.isoformat()
        )
        self.assertEqual(
            "2024-08-01T00:59:00-07:00", transformed_reads[0].install_date.isoformat()
        )
        self.assertListEqual(expected_reads, transformed_reads)

    def test_transform_meters_and_reads__two_different_meters(self):
        raw_meters_with_reads = [
            beacon_meter_and_read_factory(
                account_id="1",
                meter_id="10101",
                endpoint_id="130615549",
                flow_time="2024-08-01 00:59",
                meter_install_date="2016-01-01 23:59",
            ),
            beacon_meter_and_read_factory(
                account_id="303022",
                meter_id="1470158170",
                endpoint_id="999",
                flow_time="2024-08-01 01:59",
                meter_install_date="2016-01-01 23:59",
            ),
        ]
        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(raw_meters_with_reads)
        )
        transformed_meters = list(sorted(transformed_meters, key=lambda m: m.device_id))
        transformed_reads = list(sorted(transformed_reads, key=lambda m: m.device_id))

        expected_meters = [
            GeneralMeter(
                org_id="test-org",
                device_id="130615549",
                account_id="1",
                location_id="1",
                meter_id="10101",
                endpoint_id="130615549",
                meter_install_date=self.adapter.org_timezone.localize(
                    datetime.datetime(2016, 1, 1, 23, 59)
                ),
                meter_size="0.625",
                meter_manufacturer="BADGER",
                multiplier=None,
                location_address="5391 E. MYSTREET",
                location_city="Apple",
                location_state="CA",
                location_zip="93727",
            ),
            GeneralMeter(
                org_id="test-org",
                device_id="999",
                account_id="303022",
                location_id="303022",
                meter_id="1470158170",
                endpoint_id="999",
                meter_install_date=self.adapter.org_timezone.localize(
                    datetime.datetime(2016, 1, 1, 23, 59)
                ),
                meter_size="0.625",
                meter_manufacturer="BADGER",
                multiplier=None,
                location_address="5391 E. MYSTREET",
                location_city="Apple",
                location_state="CA",
                location_zip="93727",
            ),
        ]
        self.assertListEqual(expected_meters, transformed_meters)

        expected_reads = [
            GeneralMeterRead(
                org_id="test-org",
                device_id="130615549",
                account_id="1",
                location_id="1",
                flowtime=self.adapter.org_timezone.localize(
                    datetime.datetime(2024, 8, 1, 0, 59)
                ),
                register_value=22760.0,
                register_unit="CF",
                interval_value=0.66840273,
                interval_unit="CF",
                battery="good",
                install_date=self.adapter.org_timezone.localize(
                    datetime.datetime(2024, 8, 1, 0, 59)
                ),
                connection="marginal",
                estimated=0,
            ),
            GeneralMeterRead(
                org_id="test-org",
                device_id="999",
                account_id="303022",
                location_id="303022",
                flowtime=self.adapter.org_timezone.localize(
                    datetime.datetime(2024, 8, 1, 1, 59)
                ),
                register_value=22760.0,
                register_unit="CF",
                interval_value=0.66840273,
                interval_unit="CF",
                battery="good",
                install_date=self.adapter.org_timezone.localize(
                    datetime.datetime(2024, 8, 1, 0, 59)
                ),
                connection="marginal",
                estimated=0,
            ),
        ]
        self.assertListEqual(expected_reads, transformed_reads)

    def test_transform_meters_and_reads__two_entries_for_same_meter(self):
        raw_meters_with_reads = [
            beacon_meter_and_read_factory(),
            # Second entry is the same meter but with new install date
            # We should only keep one of the entries
            beacon_meter_and_read_factory(meter_install_date="2017-02-02 23:59"),
        ]
        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(raw_meters_with_reads)
        )
        self.assertEqual(1, len(transformed_meters))
        self.assertEqual(
            self.adapter.org_timezone.localize(datetime.datetime(2017, 2, 2, 23, 59)),
            transformed_meters[0].meter_install_date,
        )
        self.assertEqual(1, len(transformed_reads))

    def test_transform_meters_and_reads__does_not_split_account_id(self):
        raw_meters_with_reads = [
            beacon_meter_and_read_factory(account_id="first-second"),
        ]
        transformed_meters, _ = self.adapter._transform_meters_and_reads(
            raw_meters_with_reads
        )
        self.assertEqual(1, len(transformed_meters))
        self.assertEqual("first-second", transformed_meters[0].account_id)

    def test_transform_meters_and_reads__ignores_reads_when_date_missing(self):
        raw_meters_with_reads = [
            beacon_meter_and_read_factory(flow_time=None),
        ]
        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(raw_meters_with_reads)
        )
        transformed_meters = list(sorted(transformed_meters, key=lambda m: m.meter_id))

        expected_meters = [
            GeneralMeter(
                org_id="test-org",
                device_id="22",
                account_id="303022",
                location_id="303022",
                meter_id="1470158170",
                endpoint_id="22",
                meter_install_date=None,
                meter_size="0.625",
                meter_manufacturer="BADGER",
                multiplier=None,
                location_address="5391 E. MYSTREET",
                location_city="Apple",
                location_state="CA",
                location_zip="93727",
            ),
        ]
        self.assertListEqual(expected_meters, transformed_meters)

        expected_reads = []
        self.assertListEqual(expected_reads, transformed_reads)


class TestBeaconReportClient(BaseTestCase):

    ENDPOINT = "/v2/eds/range"
    PARAMS = {"Start_Date": "2024-01-01", "End_Date": "2024-01-02"}
    REPORT_TEXT = "col1,col2\nval1,val2\n"

    def setUp(self):
        self.client = BeaconReportClient(user="user", password="pass")

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_range_report_status_not_finished(),
            mocked_get_range_report_status_finished(),
            mocked_get_report_from_link(text=REPORT_TEXT),
        ],
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch__returns_report_text(self, mock_sleep, mock_post, mock_get):
        result = self.client.fetch(self.ENDPOINT, self.PARAMS)
        self.assertEqual(self.REPORT_TEXT, result)

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_range_report_status_not_finished(),
            mocked_get_range_report_status_finished(),
            mocked_get_report_from_link(text=REPORT_TEXT),
        ],
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch__posts_to_correct_url(self, mock_sleep, mock_post, mock_get):
        self.client.fetch(self.ENDPOINT, self.PARAMS)
        post_call = mock_post.call_args_list[0]
        self.assertEqual(
            "https://api.beaconama.net/v2/eds/range", post_call.kwargs["url"]
        )
        self.assertEqual(self.PARAMS, post_call.kwargs["params"])

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_range_report_status_not_finished(),
            mocked_get_range_report_status_finished(),
            mocked_get_report_from_link(text=REPORT_TEXT),
        ],
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch__polls_status_then_downloads(self, mock_sleep, mock_post, mock_get):
        self.client.fetch(self.ENDPOINT, self.PARAMS)
        # Two status polls + one download
        self.assertEqual(3, mock_get.call_count)
        self.assertIn(
            "https://api.beaconama.net/v2/eds/status/",
            mock_get.call_args_list[0].kwargs["url"],
        )
        self.assertIn(
            "https://api.beaconama.net/v2/eds/status/",
            mock_get.call_args_list[1].kwargs["url"],
        )
        self.assertIn(
            "https://api.beaconama.net/v1/content/",
            mock_get.call_args_list[2].kwargs["url"],
        )
        self.assertEqual(1, mock_sleep.call_count)

    @mock.patch("requests.get", side_effect=[])
    @mock.patch("requests.post", side_effect=[mocked_response_429()])
    @mock.patch("time.sleep")
    def test_fetch__raises_on_rate_limit(self, mock_sleep, mock_post, mock_get):
        with self.assertRaises(Exception) as ctx:
            self.client.fetch(self.ENDPOINT, self.PARAMS)
        self.assertIn("Rate limit exceeded", str(ctx.exception))

    @mock.patch("requests.get", side_effect=[])
    @mock.patch("requests.post", side_effect=[mocked_response_500()])
    @mock.patch("time.sleep")
    def test_fetch__raises_on_non_202(self, mock_sleep, mock_post, mock_get):
        with self.assertRaises(Exception) as ctx:
            self.client.fetch(self.ENDPOINT, self.PARAMS)
        self.assertIn("Failed request to generate report", str(ctx.exception))

    @mock.patch("requests.get", side_effect=[mocked_response_500()])
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch__raises_when_status_poll_returns_non_200(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as ctx:
            self.client.fetch(self.ENDPOINT, self.PARAMS)
        self.assertIn("Failed request to get report status", str(ctx.exception))

    @mock.patch("requests.get", side_effect=[mocked_exception_from_status_check()])
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch__raises_when_status_indicates_exception(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as ctx:
            self.client.fetch(self.ENDPOINT, self.PARAMS)
        self.assertIn("Exception found in report status", str(ctx.exception))

    @mock.patch(
        "requests.get",
        side_effect=[mocked_get_range_report_status_not_finished()] * 500,
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch__raises_when_max_poll_attempts_reached(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as ctx:
            self.client.fetch(self.ENDPOINT, self.PARAMS)
        self.assertIn("Reached max attempts", str(ctx.exception))

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_range_report_status_finished(),
            Exception("connection error"),
            mocked_get_report_from_link(text=REPORT_TEXT),
        ],
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch__retries_download_once_on_exception(
        self, mock_sleep, mock_post, mock_get
    ):
        result = self.client.fetch(self.ENDPOINT, self.PARAMS)
        self.assertEqual(self.REPORT_TEXT, result)
        self.assertEqual(1, mock_sleep.call_count)

    @mock.patch(
        "requests.get",
        side_effect=[mocked_get_range_report_status_finished(), mocked_response_500()],
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch__raises_when_download_returns_non_200(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as ctx:
            self.client.fetch(self.ENDPOINT, self.PARAMS)
        self.assertIn("Failed request to download report", str(ctx.exception))


class TestBeaconRawSnowflakeLoader(BaseTestCase):

    def setUp(self):
        self.conn = mock.Mock()
        self.mock_cursor = mock.Mock()
        self.conn.cursor.return_value = self.mock_cursor
        meter_and_read = beacon_meter_and_read_factory()
        leak = Beacon360LeakAlert(
            Account_ID="303022",
            Endpoint_SN="1470158170",
            Current_Leak_Start_Date="2024-08-01 00:00",
            Current_Leak_Rate="1.5",
            Current_Leak_Unit="Gallons Per Hour",
        )
        exception = Beacon360Exception(
            Account_ID="303022",
            Endpoint_SN="1470158170",
            Exception_Start_Date="2024-08-01 00:00",
            Exception_End_Date="2024-08-02 00:00",
            Exception="Tamper",
        )
        self.extract_outputs = ExtractOutput(
            {
                "meters_and_reads.json": json.dumps(
                    meter_and_read, cls=DataclassJSONEncoder
                ),
                "leaks.json": json.dumps(leak, cls=DataclassJSONEncoder),
                "exceptions.json": json.dumps(exception, cls=DataclassJSONEncoder),
            }
        )

    def test_load(self):
        loader = BEACON_RAW_SNOWFLAKE_LOADER
        loader.load(
            "run-id",
            "org-id",
            pytz.timezone("America/Los_Angeles"),
            self.extract_outputs,
            self.conn,
        )
        # 2 execute (CREATE temp + MERGE) per table loader × 3 loaders
        self.assertEqual(6, self.mock_cursor.execute.call_count)
        # 1 executemany (INSERT) per table loader × 3 loaders
        self.assertEqual(3, self.mock_cursor.executemany.call_count)

    def test_load__reads_table(self):
        loader = BeaconRawTableLoader()
        raw_data = loader.prepare_raw_data(self.extract_outputs)
        self.assertEqual(1, len(raw_data))
        self.assertEqual("22", raw_data[0][0])  # device_id = Endpoint_SN

    def test_load__leaks_table(self):
        loader = BeaconLeaksRawTableLoader()
        raw_data = loader.prepare_raw_data(self.extract_outputs)
        self.assertEqual(1, len(raw_data))
        self.assertEqual("1470158170", raw_data[0][0])  # Endpoint_SN
        self.assertEqual("303022", raw_data[0][1])  # Account_ID

    def test_load__leaks_table__empty_when_no_leaks_file(self):
        outputs = ExtractOutput({"meters_and_reads.json": ""})
        loader = BeaconLeaksRawTableLoader()
        raw_data = loader.prepare_raw_data(outputs)
        self.assertEqual([], raw_data)

    def test_load__exceptions_table(self):
        loader = BeaconExceptionsRawTableLoader()
        raw_data = loader.prepare_raw_data(self.extract_outputs)
        self.assertEqual(1, len(raw_data))
        self.assertEqual("1470158170", raw_data[0][0])  # Endpoint_SN
        self.assertEqual("303022", raw_data[0][1])  # Account_ID

    def test_load__exceptions_table__empty_when_no_exceptions_file(self):
        outputs = ExtractOutput({"meters_and_reads.json": ""})
        loader = BeaconExceptionsRawTableLoader()
        raw_data = loader.prepare_raw_data(outputs)
        self.assertEqual([], raw_data)

import datetime
import pytz
from unittest import mock

from amiadapters.adapters.zenner import (
    ZennerAccount,
    ZennerAccountMeter,
    ZennerAdapter,
    ZennerMeter,
    ZennerReading,
)
from amiadapters.models import GeneralMeter, GeneralMeterRead

from test.base_test_case import BaseTestCase, MockResponse, mocked_response_500


def mocked_get_all_meters(*args, **kwargs):
    data = [
        {
            "flowCapacity": None,
            "inletOutlet": None,
            "meterID": "010201",
            "meterType": "W",
            "multiplierRuleDisplay": 0,
            "readTypeCode": None,
            "technology": None,
            "truncationRuleDisplay": 0,
            "unitOfMeasure": None,
        }
    ]
    return MockResponse(data, 200)


def mocked_get_all_account_meters(*args, **kwargs):
    data = [
        {
            "accountID": "4-01162-01",
            "expireDate": "2000-01-01T00:00:00",
            "linkDate": "2022-05-11T18:54:00",
            "meterID": "010201",
            "nodeID": "2476542",
        }
    ]
    return MockResponse(data, 200)


def mocked_get_all_accounts(*args, **kwargs):
    data = [
        {
            "account_ID": "3-1226I-00",
            "address": {
                "address": "0 LORENCITA & GRAND",
                "city": "WEST COVINA",
                "state": "CA",
                "zipCode": "91791",
            },
            "cycle": "01",
            "firstReadDate": "2000-01-01T00:00:00",
            "name": "L.A. CO. DEPT OF PUB",
            "route": "S",
            "utilityType": "Water",
        }
    ]
    return MockResponse(data, 200)


def mocked_get_all_nodes(*args, **kwargs):
    data = [
        {
            "activationDate": "2025-07-28T22:00:00",
            "miU_SubType": 100,
            "miU_Type": 4,
            "outOfServiceDate": None,
            "regType": 25,
            "serialNumber": "2002568",
        }
    ]
    return MockResponse(data, 200)


def mocked_get_all_readings_first_page(*args, **kwargs):
    data = [
        {
            "account_ID": "3-00534-00",
            "lastBillRead": 15771,
            "lastReadId": 28033,
            "meter_ID": "1001",
            "read": 116233.61,
            "read_Dttm": "2024-07-07T01:00:00",
            "serialNumber": 2002787,
            "water_Mag_Disp": 1,
            "water_TruncRule_Disp": 0,
        }
    ]
    return MockResponse(data, 200)


def mocked_get_all_readings_empty(*args, **kwargs):
    return MockResponse([], 200)


class TestZennerAdapter(BaseTestCase):

    def setUp(self):
        self.adapter = ZennerAdapter(
            org_id="this-utility",
            org_timezone=pytz.UTC,
            pipeline_configuration=self.TEST_PIPELINE_CONFIGURATION,
            utility="zennerapi.this_utility.2001",
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
        self.assertEqual("zennerapi.this_utility.2001", self.adapter.utility)
        self.assertEqual("zenner-this-utility", self.adapter.name())

    def test_headers(self):
        self.assertEqual(
            {
                "UserName": "user",
                "Password": "pass",
                "utility": "zennerapi.this_utility.2001",
            },
            self.adapter._headers(),
        )

    @mock.patch("requests.get", side_effect=[mocked_get_all_meters()])
    def test_extract_all_meters(self, mock_get):
        result = self.adapter._extract_all_meters()
        self.assertEqual(1, len(result))
        meter = result[0]
        self.assertEqual("010201", meter.meter_id)
        self.assertEqual("W", meter.meter_type)
        self.assertEqual(None, meter.flow_capacity)
        self.assertEqual(None, meter.unit_of_measure)
        self.assertEqual(0, meter.multiplier_rule_display)
        mock_get.assert_called_once_with(
            "https://api.stealthami.com/api/Meters/GetAllMeters",
            headers=self.adapter._headers(),
            params=None,
        )

    @mock.patch("requests.get", side_effect=[mocked_get_all_account_meters()])
    def test_extract_all_account_meters(self, mock_get):
        result = self.adapter._extract_all_account_meters()
        self.assertEqual(1, len(result))
        am = result[0]
        self.assertEqual("4-01162-01", am.account_id)
        self.assertEqual("010201", am.meter_id)
        self.assertEqual("2476542", am.node_id)
        self.assertEqual("2022-05-11T18:54:00", am.link_date)
        self.assertEqual("2000-01-01T00:00:00", am.expire_date)
        # Unique key is synthesized from account + meter + node.
        self.assertEqual("4-01162-01-010201-2476542", am.unique_key)

    @mock.patch("requests.get", side_effect=[mocked_get_all_accounts()])
    def test_extract_all_accounts_flattens_address(self, mock_get):
        result = self.adapter._extract_all_accounts()
        self.assertEqual(1, len(result))
        account = result[0]
        self.assertEqual("3-1226I-00", account.account_id)
        self.assertEqual("0 LORENCITA & GRAND", account.address)
        self.assertEqual("WEST COVINA", account.city)
        self.assertEqual("CA", account.state)
        self.assertEqual("91791", account.zip_code)
        self.assertEqual("Water", account.utility_type)

    @mock.patch("requests.get", side_effect=[mocked_get_all_nodes()])
    def test_extract_all_nodes(self, mock_get):
        result = self.adapter._extract_all_nodes()
        self.assertEqual(1, len(result))
        node = result[0]
        self.assertEqual("2002568", node.serial_number)
        self.assertEqual(4, node.miu_type)
        self.assertEqual(100, node.miu_subtype)
        self.assertEqual(25, node.register_type)
        self.assertEqual("2025-07-28T22:00:00", node.activation_date)

    @mock.patch("requests.get", side_effect=[mocked_response_500()])
    def test_extract__non_200_raises(self, mock_get):
        with self.assertRaises(Exception):
            self.adapter._extract_all_nodes()

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_all_readings_first_page(),
            mocked_get_all_readings_empty(),
        ],
    )
    def test_extract_all_reads_paginates(self, mock_get):
        # First page returns fewer than READS_PAGE_SIZE, so a single page is enough,
        # but force a second call by shrinking the page size.
        self.adapter.READS_PAGE_SIZE = 1
        result = self.adapter._extract_all_reads(self.range_start, self.range_end)
        self.assertEqual(1, len(result))
        self.assertEqual(
            ZennerReading(
                last_read_id=28033,
                account_id="3-00534-00",
                meter_id="1001",
                serial_number="2002787",
                flowtime="2024-07-07T01:00:00",
                reading=116233.61,
                last_bill_read=15771,
                multiplier_display=1,
                truncation_rule_display=0,
            ),
            result[0],
        )
        first_call = mock_get.call_args_list[0]
        self.assertEqual(
            {
                "StartDateTime": "2026-01-25",
                "EndDateTime": "2026-01-26",
                "NumRecords": 1,
                "StartReadingID": 0,
            },
            first_call.kwargs["params"],
        )
        # Offset advanced by the number of records returned on page one.
        self.assertEqual(
            1, mock_get.call_args_list[1].kwargs["params"]["StartReadingID"]
        )

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_all_readings_first_page(),
            mocked_get_all_readings_first_page(),
        ],
    )
    def test_extract_all_reads_stops_when_page_repeats(self, mock_get):
        # Simulate an endpoint that ignores StartReadingID and re-returns the same full
        # page. The dedup/no-progress guard must stop instead of looping forever.
        self.adapter.READS_PAGE_SIZE = 1
        result = self.adapter._extract_all_reads(self.range_start, self.range_end)
        self.assertEqual(1, len(result))
        # Two calls: the first page, then a repeat that yields no new reads and breaks.
        self.assertEqual(2, mock_get.call_count)

    def test_transform_meters_and_reads(self):
        meters = [
            ZennerMeter(
                meter_id="1001",
                meter_type="Positive Displacement",
                flow_capacity='3/4"',
                inlet_outlet="Inlet",
                # The API never provides a unit; the transform should default it to CF.
                unit_of_measure=None,
                multiplier_rule_display="1",
                truncation_rule_display="None",
                technology="AMI",
                read_type_code="R",
            )
        ]
        account_meters = [
            ZennerAccountMeter(
                account_id="5",
                meter_id="1001",
                node_id="90",
                link_date="2022-02-08T20:16:41",
                expire_date=None,
                unique_key="5-1001-90",
            )
        ]
        accounts = [
            ZennerAccount(
                account_id="5",
                name="JANE DOE",
                address="10 SW MYROAD RD",
                city="MY TOWN",
                state="NM",
                zip_code="10101",
                first_read_date="2022-02-09T00:00:00",
                utility_type="Water",
                cycle="1",
                route="1",
            ),
            # A non-water account that should be filtered out of the lookup.
            ZennerAccount(
                account_id="6",
                name="GAS CO",
                address="99 GAS LN",
                city="MY TOWN",
                state="NM",
                zip_code="10101",
                first_read_date="2022-02-09T00:00:00",
                utility_type="Gas",
                cycle="1",
                route="1",
            ),
        ]
        reads = [
            ZennerReading(
                last_read_id=28033,
                # The read's own account_id is used directly for the transformed read.
                account_id="999",
                meter_id="1001",
                serial_number="2002787",
                flowtime="2024-07-07T01:00:00",
                reading=116233.61,
                last_bill_read=15771,
                multiplier_display=1,
                truncation_rule_display=0,
            )
        ]

        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(
                meters, account_meters, accounts, reads
            )
        )

        self.assertEqual(
            [
                GeneralMeter(
                    org_id="this-utility",
                    device_id="1001",
                    account_id="5",
                    location_id="5",
                    meter_id="1001",
                    endpoint_id="90",
                    meter_install_date=self.adapter.org_timezone.localize(
                        datetime.datetime(2022, 2, 8, 20, 16, 41)
                    ),
                    meter_size=None,
                    meter_manufacturer=None,
                    multiplier=1.0,
                    location_address="10 SW MYROAD RD",
                    location_city="MY TOWN",
                    location_state="NM",
                    location_zip="10101",
                )
            ],
            transformed_meters,
        )

        self.assertEqual(
            [
                GeneralMeterRead(
                    org_id="this-utility",
                    device_id="1001",
                    account_id="999",
                    location_id="999",
                    flowtime=self.adapter.org_timezone.localize(
                        datetime.datetime(2024, 7, 7, 1, 0)
                    ),
                    register_value=116233.61,
                    register_unit="CF",
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

    def test_transform_dedupes_duplicate_accounts(self):
        # GetAllAccounts returns one row per account-meter association, so an account with
        # multiple meters appears multiple times. Identical rows must collapse to a single
        # account in the meter lookup.
        meters = [
            ZennerMeter(
                meter_id="1001",
                meter_type="W",
                flow_capacity=None,
                inlet_outlet=None,
                unit_of_measure=None,
                multiplier_rule_display=0,
                truncation_rule_display=0,
                technology=None,
                read_type_code=None,
            )
        ]
        account_meters = [
            ZennerAccountMeter(
                account_id="3-1226I-00",
                meter_id="1001",
                node_id="2252273",
                link_date="2018-08-16T14:06:00",
                expire_date="2018-08-16T14:04:00",
                unique_key="3-1226I-00-1001-2252273",
            )
        ]
        # The same account repeated four times (one per linked meter).
        duplicate_account = ZennerAccount(
            account_id="3-1226I-00",
            name="L.A. CO. DEPT OF PUB",
            address="0 LORENCITA & GRAND",
            city="WEST COVINA",
            state="CA",
            zip_code="91791",
            first_read_date="2000-01-01T00:00:00",
            utility_type="Water",
            cycle="01",
            route="S",
        )
        accounts = [
            duplicate_account,
            duplicate_account,
            duplicate_account,
            duplicate_account,
        ]

        transformed_meters, _ = self.adapter._transform_meters_and_reads(
            meters, account_meters, accounts, []
        )

        # One meter, with the account's address resolved despite the duplicate rows.
        self.assertEqual(1, len(transformed_meters))
        meter = transformed_meters[0]
        self.assertEqual("3-1226I-00", meter.account_id)
        self.assertEqual("0 LORENCITA & GRAND", meter.location_address)
        self.assertEqual("WEST COVINA", meter.location_city)
        self.assertEqual("CA", meter.location_state)
        self.assertEqual("91791", meter.location_zip)

    def test_transform_raises_on_duplicate_account_meter_meter_id(self):
        # We assume account meters are unique by meter_id; if that assumption breaks the
        # transform must fail loudly rather than silently pick an arbitrary association.
        meters = [
            ZennerMeter(
                meter_id="1001",
                meter_type="W",
                flow_capacity=None,
                inlet_outlet=None,
                unit_of_measure=None,
                multiplier_rule_display=0,
                truncation_rule_display=0,
                technology=None,
                read_type_code=None,
            )
        ]
        account_meters = [
            ZennerAccountMeter(
                account_id="5",
                meter_id="1001",
                node_id="90",
                link_date="2018-08-16T14:06:00",
                expire_date="2019-01-01T00:00:00",
                unique_key="5-1001-90",
            ),
            ZennerAccountMeter(
                account_id="6",
                meter_id="1001",
                node_id="91",
                link_date="2020-01-01T00:00:00",
                expire_date=None,
                unique_key="6-1001-91",
            ),
        ]

        with self.assertRaises(ValueError):
            self.adapter._transform_meters_and_reads(meters, account_meters, [], [])

    def test_transform_skips_meter_missing_meter_id(self):
        meters = [
            ZennerMeter(
                meter_id=None,
                meter_type="W",
                flow_capacity=None,
                inlet_outlet=None,
                unit_of_measure=None,
                multiplier_rule_display=0,
                truncation_rule_display=0,
                technology=None,
                read_type_code=None,
            ),
            ZennerMeter(
                meter_id="1001",
                meter_type="W",
                flow_capacity=None,
                inlet_outlet=None,
                unit_of_measure=None,
                multiplier_rule_display=0,
                truncation_rule_display=0,
                technology=None,
                read_type_code=None,
            ),
        ]

        transformed_meters, _ = self.adapter._transform_meters_and_reads(
            meters, [], [], []
        )

        # The meter with no meter_id is skipped; the valid one remains.
        self.assertEqual(1, len(transformed_meters))
        self.assertEqual("1001", transformed_meters[0].meter_id)

    def test_transform_skips_read_missing_meter_id_or_flowtime(self):
        reads = [
            ZennerReading(
                last_read_id=1,
                account_id="5",
                meter_id=None,
                serial_number="2002787",
                flowtime="2024-07-07T01:00:00",
                reading=1,
                last_bill_read=1,
                multiplier_display=1,
                truncation_rule_display=0,
            ),
            ZennerReading(
                last_read_id=2,
                account_id="5",
                meter_id="1001",
                serial_number="2002787",
                flowtime=None,
                reading=2,
                last_bill_read=2,
                multiplier_display=1,
                truncation_rule_display=0,
            ),
            ZennerReading(
                last_read_id=3,
                account_id="5",
                meter_id="1001",
                serial_number="2002787",
                flowtime="2024-07-07T01:00:00",
                reading=3,
                last_bill_read=3,
                multiplier_display=1,
                truncation_rule_display=0,
            ),
        ]

        _, transformed_reads = self.adapter._transform_meters_and_reads(
            [], [], [], reads
        )

        # Only the read with both meter_id and flowtime survives.
        self.assertEqual(1, len(transformed_reads))
        self.assertEqual("1001", transformed_reads[0].device_id)
        self.assertEqual(3, transformed_reads[0].register_value)

    def test_transform_meter_alerts_is_empty(self):
        self.assertEqual([], self.adapter._transform_meter_alerts("run", None))

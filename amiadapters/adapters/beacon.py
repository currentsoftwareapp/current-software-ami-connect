import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import StringIO
import json
import logging
import os
import time
from typing import Generator, List, Tuple

from pytz.tzinfo import DstTzInfo
import requests

from amiadapters.adapters.base import (
    BaseAMIAdapter,
    ScheduledExtract,
    STANDARD_DAILY_SCHEDULED_EXTRACT,
)
from amiadapters.configuration.models import MetricsConfigurationBase
from amiadapters.models import (
    DataclassJSONEncoder,
    GeneralMeter,
    GeneralMeterAlert,
    GeneralMeterRead,
    MeterAlertSource,
)
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader, RawSnowflakeTableLoader

logger = logging.getLogger(__name__)

METER_ALERT_STALE_THRESHOLD = timedelta(days=30)


@dataclass
class Beacon360MeterAndRead:
    """
    Representation of row in the Beacon 360 range report CSV, which includes
    meter metadata and meter read data.

    NOTE: We make the attribute names match the column names in the Beacon 360 API call and resulting CSV
    for code convenience.
    """

    Account_ID: str
    Backflow_Gallons: str
    Battery_Level: str
    Current_Leak_Rate: str
    Current_Leak_Start_Date: str
    Demand_Zone_ID: str
    Dials: str
    Endpoint_Install_Date: str
    Endpoint_SN: str
    Estimated_Flag: str
    Flow: str
    Flow_Time: str
    Flow_Unit: str
    Location_Address_Line1: str
    Location_Address_Line2: str
    Location_Address_Line3: str
    Location_City: str
    Location_Country: str
    Location_ID: str
    Location_State: str
    Location_ZIP: str
    Location_Continuous_Flow: str
    Location_Latitude: str
    Location_Longitude: str
    Location_Irrigated_Area: str
    Location_Irrigation: str
    Location_Main_Use: str
    Location_Name: str
    Location_Pool: str
    Location_Water_Type: str
    Location_Year_Built: str
    Meter_ID: str
    Meter_Install_Date: str
    Meter_Manufacturer: str
    Meter_Model: str
    Meter_Size: str
    Meter_Size_Desc: str
    Meter_Size_Unit: str
    Meter_SN: str
    Raw_Read: str
    Read: str
    Read_Time: str
    Read_Unit: str
    Register_Number: str
    Register_Resolution: str
    SA_Start_Date: str
    Service_Point_Class_Code: str
    Service_Point_Class_Code_Normalized: str
    Signal_Strength: str


# Columns we'll request from Beacon 360 meter reads API
REQUESTED_COLUMNS_FOR_READS = list(Beacon360MeterAndRead.__dataclass_fields__.keys())


@dataclass
class Beacon360LeakAlert:
    """
    Representation of a row in the Beacon 360 leak report CSV.

    NOTE: Attribute names match the column names in the Beacon 360 API response for
    code convenience.
    """

    Account_ID: str
    Endpoint_SN: str
    Current_Leak_Start_Date: str
    Current_Leak_Rate: str
    Current_Leak_Unit: str


# Columns we'll request from Beacon 360 leaks API
REQUESTED_COLUMNS_FOR_LEAKS = list(Beacon360LeakAlert.__dataclass_fields__.keys())


@dataclass
class Beacon360Exception:
    """
    Representation of a row in the Beacon 360 exceptions report CSV.

    NOTE: Attribute names match the column names in the Beacon 360 API response for
    code convenience.
    """

    Account_ID: str
    Endpoint_SN: str
    Exception_Start_Date: str
    Exception_End_Date: str
    Exception: str


# Columns we'll request from Beacon 360 exceptions API
REQUESTED_COLUMNS_FOR_EXCEPTIONS = list(Beacon360Exception.__dataclass_fields__.keys())


class Beacon360Adapter(BaseAMIAdapter):
    """
    AMI Adapter that retrieves data from the Beacon 360 V2 API.

    API Documentation: https://helpbeaconama.net/beacon-web-services/export-data-service-v2-api-preview/#POSTread
    """

    def __init__(
        self,
        api_user: str,
        api_password: str,
        use_cache: bool,
        org_id: str,
        org_timezone: DstTzInfo,
        pipeline_configuration,
        configured_task_output_controller,
        configured_meter_alerts,
        configured_metrics: MetricsConfigurationBase,
        configured_sinks,
        cache_output_folder: str = "./output",
    ):
        self.user = api_user
        self.password = api_password
        self.use_cache = use_cache
        self.cache_output_folder = cache_output_folder
        self.report_client = BeaconReportClient(api_user, api_password)
        super().__init__(
            org_id,
            org_timezone,
            pipeline_configuration,
            configured_task_output_controller,
            configured_meter_alerts,
            configured_metrics,
            configured_sinks,
            BEACON_RAW_SNOWFLAKE_LOADER,
        )

    def name(self) -> str:
        return f"beacon-360-{self.org_id}"

    def scheduled_extracts(self) -> List[ScheduledExtract]:
        """
        We've seen that sources using the Beacon adapter don't get their full set of reads with
        the standard daily polling - they get maybe 80% of the reads. Many more show up 3+ days
        after flowtime. Still more show up 5+ months after flowtime.
        We use lagged scheduled extracts to go back for the full set of reads.
        """
        return [
            # Get the last couple days of readings
            STANDARD_DAILY_SCHEDULED_EXTRACT,
            # Re-retrieve readings from a couple weeks ago
            ScheduledExtract(
                name="lagged",
                interval=timedelta(days=1),
                lag=timedelta(days=14),
                schedule_crontab="0 10 * * *",
            ),
            # Re-retrieve readings from many months ago
            ScheduledExtract(
                name="half-year-lagged",
                interval=timedelta(days=1),
                lag=timedelta(days=6 * 30),
                schedule_crontab="0 11 * * *",
            ),
        ]

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ):
        report = self._fetch_range_report(
            extract_range_start,
            extract_range_end,
        )
        logger.info("Fetched report")

        alerts = self._fetch_exceptions_report(
            extract_range_start,
            extract_range_end,
        )
        logger.info("Fetched alerts")

        leaks = self._fetch_leaks_report()
        logger.info("Fetched leaks")

        return ExtractOutput(
            {
                "meters_and_reads.json": self._report_to_output(report),
                "leaks.json": self._leaks_report_to_output(leaks),
                "exceptions.json": self._exceptions_report_to_output(alerts),
            }
        )

    def _fetch_range_report(
        self,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> str:
        """
        Return range report as CSV string, first line with headers.
        Retrieve from cache if configured to do so.
        """
        if self.use_cache:
            logger.info("Attempting to load report from cache")
            cached_report = self._get_cached_report(
                extract_range_start, extract_range_end
            )
            if cached_report is not None:
                logger.info("Loaded report from cache")
                return cached_report
            logger.info(
                "Could not load report from cache, continuing with calls to API"
            )

        params = {
            "Start_Date": extract_range_start,
            "End_Date": extract_range_end,
            "Resolution": "hourly",
            "Header_Columns": ",".join(REQUESTED_COLUMNS_FOR_READS),
            "Has_Endpoint": True,
        }
        logger.info(
            f"Requesting report for meter reads between {extract_range_start} and {extract_range_end} at hourly resolution"
        )
        report = self.report_client.fetch("/v2/eds/range", params)
        self._write_cached_report_and_delete_old_cached_files(
            report, extract_range_start, extract_range_end
        )
        return report

    def _fetch_exceptions_report(
        self,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> str:
        """
        Return exceptions report w/ meter exceptions as CSV string, first line with headers.
        Exceptions include tamper alerts, encoder alerts, etc.
        """
        params = {
            "Start_Date": extract_range_start,
            "End_Date": extract_range_end,
            "Header_Columns": ",".join(REQUESTED_COLUMNS_FOR_EXCEPTIONS),
        }
        logger.info(
            f"Requesting report for meter exceptions between {extract_range_start} and {extract_range_end}"
        )
        return self.report_client.fetch(
            "/v2/eds/exception_range", params, sleep_interval_seconds=15
        )

    def _fetch_leaks_report(self) -> str:
        """
        Return leaks report as CSV string, first line with headers.
        """
        params = {
            "Header_Columns": ",".join(REQUESTED_COLUMNS_FOR_LEAKS),
        }
        logger.info("Requesting report for leaks")
        return self.report_client.fetch(
            "/v2/eds/leak", params, sleep_interval_seconds=5
        )

    def _report_to_output(self, report: str):
        return "\n".join(self._report_to_output_stream(report))

    def _report_to_output_stream(self, report: str) -> Generator[str, None, None]:
        csv_reader = csv.DictReader(StringIO(report), delimiter=",")
        for data in csv_reader:
            meter_and_read = Beacon360MeterAndRead(**data)
            yield json.dumps(meter_and_read, cls=DataclassJSONEncoder)

    def _leaks_report_to_output(self, report: str) -> str:
        return "\n".join(self._leaks_report_to_output_stream(report))

    def _leaks_report_to_output_stream(self, report: str) -> Generator[str, None, None]:
        csv_reader = csv.DictReader(StringIO(report), delimiter=",")
        for data in csv_reader:
            leak = Beacon360LeakAlert(**data)
            yield json.dumps(leak, cls=DataclassJSONEncoder)

    def _exceptions_report_to_output(self, report: str) -> str:
        return "\n".join(self._exceptions_report_to_output_stream(report))

    def _exceptions_report_to_output_stream(
        self, report: str
    ) -> Generator[str, None, None]:
        csv_reader = csv.DictReader(StringIO(report), delimiter=",")
        for data in csv_reader:
            exception = Beacon360Exception(**data)
            yield json.dumps(exception, cls=DataclassJSONEncoder)

    def _get_cached_report(
        self, extract_range_start: datetime, extract_range_end: datetime
    ) -> str:
        cache_file = self._cached_report_file(extract_range_start, extract_range_end)
        if os.path.exists(cache_file):
            with open(cache_file, "r") as f:
                return f.read()
        return None

    def _write_cached_report_and_delete_old_cached_files(
        self, report: str, extract_range_start: datetime, extract_range_end: datetime
    ):
        cache_file = self._cached_report_file(extract_range_start, extract_range_end)
        logger.info(f"Caching report contents at {cache_file}")
        directory = os.path.dirname(cache_file)
        # Create all necessary parent directories
        if directory:
            logger.info(f"Making parent directories for {cache_file}")
            os.makedirs(directory, exist_ok=True)

        # Remove old cache files so we don't fill up the disk
        previous_cache_files = [
            os.path.join(self.cache_output_folder, f)
            for f in os.listdir(directory)
            if "cached-report" in f
        ]
        for f in previous_cache_files:
            os.remove(f)
            logger.info(f"Deleted old cache file {f}")

        with open(cache_file, "w") as f:
            f.write(report)
        logger.info(f"Cached report contents at {cache_file}")

    def _transform(self, run_id: str, extract_outputs: ExtractOutput):
        raw_meters_with_reads = extract_outputs.load_from_file(
            "meters_and_reads.json", Beacon360MeterAndRead
        )
        return self._transform_meters_and_reads(raw_meters_with_reads)

    def _transform_meters_and_reads(
        self, raw_meters_with_reads: List[Beacon360MeterAndRead]
    ) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        transformed_meters_by_device_id = {}
        transformed_reads_by_key = {}
        for meter_and_read in raw_meters_with_reads:
            account_id = meter_and_read.Account_ID
            location_id = meter_and_read.Location_ID
            device_id = meter_and_read.Endpoint_SN

            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account_id,
                location_id=location_id,
                meter_id=meter_and_read.Meter_ID,
                endpoint_id=meter_and_read.Endpoint_SN,
                meter_install_date=self.datetime_from_iso_str(
                    meter_and_read.Meter_Install_Date, self.org_timezone
                ),
                meter_size=self.map_meter_size(meter_and_read.Meter_Size_Desc),
                meter_manufacturer=meter_and_read.Meter_Manufacturer,
                multiplier=None,
                location_address=meter_and_read.Location_Address_Line1,
                location_city=meter_and_read.Location_City,
                location_state=meter_and_read.Location_State,
                location_zip=meter_and_read.Location_ZIP,
            )

            transformed_meters_by_device_id[device_id] = meter

            flowtime = self.datetime_from_iso_str(
                meter_and_read.Read_Time, self.org_timezone
            )
            if flowtime is None:
                logger.info(
                    f"Skipping read with no flowtime for account={account_id} location={location_id} device={device_id}"
                )
                continue

            register_value, register_unit = self.map_reading(
                float(meter_and_read.Read),
                meter_and_read.Read_Unit,  # Expected to be CCF or KGAL
            )

            interval_value, interval_unit = self.map_reading(
                float(meter_and_read.Flow),
                meter_and_read.Flow_Unit,  # Expected to be CCF or KGAL
            )

            read = GeneralMeterRead(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account_id,
                location_id=location_id,
                flowtime=flowtime,
                register_value=register_value,
                register_unit=register_unit,
                interval_value=interval_value,
                interval_unit=interval_unit,
                battery=meter_and_read.Battery_Level,
                install_date=self.datetime_from_iso_str(
                    meter_and_read.Endpoint_Install_Date, self.org_timezone
                ),
                connection=meter_and_read.Signal_Strength,
                estimated=(
                    int(meter_and_read.Estimated_Flag)
                    if meter_and_read.Estimated_Flag is not None
                    else None
                ),
            )
            # Reads are unique by org_id, device_id, and flowtime. This ensures we do not include duplicates in our output.
            key = f"{read.device_id}-{read.flowtime}"
            transformed_reads_by_key[key] = read

        return list(transformed_meters_by_device_id.values()), list(
            transformed_reads_by_key.values()
        )

    def _transform_meter_alerts(self, run_id, extract_outputs):
        result = []

        exceptions = extract_outputs.load_from_file(
            "exceptions.json", Beacon360Exception, allow_empty=True
        )
        for exception in exceptions:
            if not exception.Endpoint_SN:
                logger.info(
                    f"Skipping exception with missing Endpoint_SN: type={exception.Exception} start={exception.Exception_Start_Date}"
                )
                continue

            start_time = self.datetime_from_iso_str(
                exception.Exception_Start_Date, self.org_timezone
            )
            if not start_time:
                logger.warning(
                    f"Skipping exception with missing or invalid Exception_Start_Date: type={exception.Exception} device={exception.Endpoint_SN}"
                )
                continue

            end_time = self.datetime_from_iso_str(
                exception.Exception_End_Date, self.org_timezone
            )

            if capped_end_time := self._calculate_capped_alert_end_time(
                start_time,
                end_time,
            ):
                logger.info(
                    f"Capping stale open {exception.Exception} for {exception.Endpoint_SN} started {start_time}: setting end_time to {capped_end_time}"
                )
                end_time = capped_end_time

            result.append(
                GeneralMeterAlert(
                    org_id=self.org_id,
                    device_id=exception.Endpoint_SN,
                    alert_type=exception.Exception,
                    start_time=start_time,
                    end_time=end_time,
                    source=MeterAlertSource.BEACON_360,
                )
            )

        return result

    def _calculate_capped_alert_end_time(
        self, start_time: datetime, end_time: datetime
    ) -> datetime:
        """
        Cap meter alert end time to avoid having stale open alerts in our system.
        Meter alerts may be left open indefinitely in the Beacon 360 system
        even if the underlying issue has been resolved.
        """
        if (
            end_time is None
            and start_time is not None
            and (datetime.now(tz=start_time.tzinfo) - start_time)
            > METER_ALERT_STALE_THRESHOLD
        ):
            return start_time + METER_ALERT_STALE_THRESHOLD
        return None

    def _cached_report_file(
        self, extract_range_start: datetime, extract_range_end: datetime
    ) -> str:
        start, end = extract_range_start.isoformat(), extract_range_end.isoformat()
        return os.path.join(
            self.cache_output_folder, f"{self.name()}-{start}-{end}-cached-report.txt"
        )


class BeaconReportClient:
    """
    Handles the full async lifecycle of a Beacon 360 report:
    create async report → poll for report status → download content.
    """

    BASE_URL = "https://api.beaconama.net"
    HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}

    def __init__(self, user: str, password: str):
        self.auth = requests.auth.HTTPBasicAuth(user, password)

    def fetch(
        self, endpoint: str, params: dict, sleep_interval_seconds: int = 60
    ) -> str:
        """
        POST to the given endpoint to trigger report generation, poll until done,
        then download and return the report text.
        """
        generate_response = requests.post(
            url=f"{self.BASE_URL}{endpoint}",
            headers=self.HEADERS,
            params=params,
            auth=self.auth,
            timeout=120,
        )

        if generate_response.status_code == 429:
            t = generate_response.json()
            if len(t.get("args", [])) > 2:
                secs_to_wait = int(t["args"][2])
                time_to_resume = datetime.now() + timedelta(seconds=secs_to_wait)
                logger.warning(
                    f"need to wait {secs_to_wait} seconds until {time_to_resume} ({t})"
                )
            raise Exception(f"Rate limit exceeded. Message from Beacon API: {t}")
        elif generate_response.status_code != 202:
            raise Exception(
                f"Failed request to generate report. status code: {generate_response.status_code}"
            )

        status_url = generate_response.json()["statusUrl"]
        status_data = self._poll_status(status_url, sleep_interval_seconds)

        report_url = status_data["reportUrl"]
        return self._download(report_url)

    def _poll_status(self, status_url: str, sleep_interval_seconds: int) -> dict:
        max_attempts = 120
        for i in range(1, max_attempts + 1):
            logger.info(
                f"Attempt {i}/{max_attempts} while polling for status on report at {status_url}"
            )

            status_response = requests.get(
                url=f"{self.BASE_URL}{status_url}",
                headers=self.HEADERS,
                auth=self.auth,
                timeout=120,
            )

            if status_response.status_code != 200:
                raise Exception(
                    f"Failed request to get report status. status code: {status_response.status_code} message: {status_response.text}"
                )

            status_data = status_response.json()
            logger.info(f"Status: {status_data}")

            if status_data.get("state") == "done":
                return status_data
            elif status_data.get("state") == "exception":
                raise Exception(
                    f"Exception found in report status: {status_data.get('message')}"
                )

            logger.info(f"Sleeping for {sleep_interval_seconds} seconds")
            time.sleep(sleep_interval_seconds)

        raise Exception(
            f"Reached max attempts ({max_attempts}) polling for report status"
        )

    def _download(self, report_url: str) -> str:
        try:
            logger.info(f"Downloading report at {report_url}")
            response = requests.get(
                url=f"{self.BASE_URL}{report_url}",
                headers=self.HEADERS,
                auth=self.auth,
                timeout=120,
            )
        except Exception as e:
            logger.info(f"Exception downloading report at {report_url}: {e}. Retrying.")
            time.sleep(60)
            logger.info(f"Retrying download for report at {report_url}")
            response = requests.get(
                url=f"{self.BASE_URL}{report_url}",
                headers=self.HEADERS,
                auth=self.auth,
                timeout=120,
            )

        if response.status_code != 200:
            raise Exception(
                f"Failed request to download report. status code: {response.status_code}"
            )

        return response.text


class BeaconRawTableLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "beacon_360_base"

    def columns(self) -> List[str]:
        return ["device_id"] + REQUESTED_COLUMNS_FOR_READS

    def unique_by(self) -> List[str]:
        return ["device_id", "read_time"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file(
            "meters_and_reads.json", Beacon360MeterAndRead
        )
        return [
            tuple(
                [i.Endpoint_SN]
                + [i.__getattribute__(name) for name in REQUESTED_COLUMNS_FOR_READS]
            )
            for i in raw_data
        ]


class BeaconLeaksRawTableLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "beacon_360_leaks_base"

    def columns(self) -> List[str]:
        return ["device_id"] + REQUESTED_COLUMNS_FOR_LEAKS

    def unique_by(self) -> List[str]:
        return ["device_id", "current_leak_start_date"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file(
            "leaks.json", Beacon360LeakAlert, allow_empty=True
        )
        return [
            tuple(
                [i.Endpoint_SN]
                + [i.__getattribute__(name) for name in REQUESTED_COLUMNS_FOR_LEAKS]
            )
            for i in raw_data
        ]


class BeaconExceptionsRawTableLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "beacon_360_exceptions_base"

    def columns(self) -> List[str]:
        return ["device_id"] + REQUESTED_COLUMNS_FOR_EXCEPTIONS

    def unique_by(self) -> List[str]:
        return ["device_id", "exception", "exception_start_date"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file(
            "exceptions.json", Beacon360Exception, allow_empty=True
        )
        return [
            tuple(
                [i.Endpoint_SN]
                + [
                    i.__getattribute__(name)
                    for name in REQUESTED_COLUMNS_FOR_EXCEPTIONS
                ]
            )
            for i in raw_data
        ]


BEACON_RAW_SNOWFLAKE_LOADER = RawSnowflakeLoader.with_table_loaders(
    [
        BeaconRawTableLoader(),
        BeaconLeaksRawTableLoader(),
        BeaconExceptionsRawTableLoader(),
    ]
)

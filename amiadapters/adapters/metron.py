from dataclasses import dataclass
from datetime import datetime
import json
import logging
from typing import Dict, List, Optional, Tuple

import requests

from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.models import (
    DataclassJSONEncoder,
    GeneralMeter,
    GeneralMeterRead,
    GeneralMeterUnitOfMeasure,
)
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader, RawSnowflakeTableLoader

logger = logging.getLogger(__name__)


BASE_URL = "https://webapi.waterscope.us"


@dataclass
class MetronReading:
    """
    One cumulative register read for a Metron meter (there is no interval/hourly history).
    """

    meter_id: str
    lcd_read: str
    billing_read: str
    read_date: str
    unit: str
    reference: str
    account_name: str
    address: str
    utility_defined: str


class MetronAdapter(BaseAMIAdapter):
    """
    AMI Adapter for Metron Farnier's WaterScope Web API.

    The WaterScope API does not expose interval (hourly) reads. It returns a single
    cumulative register read per meter, so this adapter produces daily/monthly billing
    reads only.
    """

    DATE_FORMAT = "%m/%d/%Y"

    UNIT_MAP = {
        "G": GeneralMeterUnitOfMeasure.GAL,
    }

    def __init__(
        self,
        org_id: str,
        org_timezone,
        pipeline_configuration,
        username: str,
        password: str,
        configured_task_output_controller,
        configured_meter_alerts,
        configured_metrics,
        configured_sinks,
    ):
        self.username = username
        self.password = password
        super().__init__(
            org_id,
            org_timezone,
            pipeline_configuration,
            configured_task_output_controller,
            configured_meter_alerts,
            configured_metrics,
            configured_sinks,
            RawSnowflakeLoader.with_table_loaders(
                [
                    MetronRawReadsLoader(),
                ]
            ),
        )

    def name(self) -> str:
        return f"metron-{self.org_id}"

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> ExtractOutput:
        reads = self._extract_reads(extract_range_start, extract_range_end)
        return ExtractOutput(
            {
                "reads.json": self._to_json(reads),
            }
        )
    
    def _get(self, path: str, params: Dict = None) -> list:
        """
        Issue a GET against the WaterScope API and return the parsed JSON body. The
        read-bearing endpoints return a top-level JSON array.

        We never log `params`: WaterScope authenticates by passing the username and
        password as query parameters, and we must not leak the password to logs.
        """
        url = f"{BASE_URL}{path}"
        response = requests.get(
            url,
            params=params,
            timeout=30,
        )
        if response.status_code != 200:
            raise Exception(
                f"Non-200 response from {path}: {response.status_code} {response.text}"
            )
        return response.json()

    @staticmethod
    def _to_json(records: list) -> str:
        return "\n".join(json.dumps(r, cls=DataclassJSONEncoder) for r in records)

    def _extract_reads(
        self, extract_range_start: datetime, extract_range_end: datetime
    ) -> List[MetronReading]:
        """
        Fetch a billing read per meter as of the end of the extract range. The API returns
        a single read per meter (not a time series), so one request covers the range.
        """
        billing_date = extract_range_end.strftime(self.DATE_FORMAT)
        days_window = (extract_range_end - extract_range_start).days
        params = {
            "username": self.username,
            "password": self.password,
            "billingDate": billing_date,
            "numberDaysWindow": days_window,
        }
        logger.info(
            f"Extracting reads for {self.org_id}, billingDate={billing_date}, "
            f"numberDaysWindow={days_window}"
        )
        raw = self._get("/api/Billing", params)
        reads = [
            MetronReading(
                meter_id=(
                    str(r.get("Meter_ID")) if r.get("Meter_ID") is not None else None
                ),
                lcd_read=self._as_str(r.get("LCD_Read")),
                billing_read=self._as_str(r.get("Billing_Read")),
                read_date=r.get("Read_Date"),
                unit=r.get("Unit"),
                reference=(
                    str(r.get("Reference")) if r.get("Reference") is not None else None
                ),
                account_name=r.get("Account_Name"),
                address=r.get("Address"),
                utility_defined=r.get("Utility_Defined"),
            )
            for r in raw
        ]
        logger.info(f"Extracted {len(reads)} reads for {self.org_id}")
        return reads

    def _transform(self, run_id: str, extract_outputs: ExtractOutput):
        raw_reads = extract_outputs.load_from_file("reads.json", MetronReading)
        return self._transform_meters_and_reads(raw_reads)

    def _transform_meters_and_reads(
        self, raw_reads: List[MetronReading]
    ) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        meters_by_id: Dict[str, GeneralMeter] = {}
        meter_reads: List[GeneralMeterRead] = []

        for raw_read in raw_reads:
            if raw_read.meter_id is None:
                logger.warning(
                    f"Skipping read with no meter_id for {self.org_id}: {raw_read}"
                )
                continue
            meter_id = str(raw_read.meter_id)

            # A single record carries both meter and account/location metadata. Build the
            # meter once (records repeat the same meter metadata across runs).
            if meter_id not in meters_by_id:
                meters_by_id[meter_id] = GeneralMeter(
                    org_id=self.org_id,
                    device_id=meter_id,
                    account_id=raw_read.reference,
                    # We have no separate location identifier from the API; reuse the
                    # account reference as the location ID, as we've done for other sources.
                    location_id=raw_read.reference,
                    meter_id=meter_id,
                    # The API exposes no MIU/radio identifier.
                    endpoint_id=None,
                    meter_install_date=None,
                    meter_size=None,
                    meter_manufacturer="Metron",
                    multiplier=None,
                    location_address=raw_read.address,
                    # The API returns a single free-text address with no city/state/zip.
                    location_city=None,
                    location_state=None,
                    location_zip=None,
                )

            flowtime = self._parse_read_date(raw_read.read_date)
            if flowtime is None:
                logger.warning(
                    f"Skipping read with missing/unparseable Read_Date for {self.org_id}: {raw_read}"
                )
                continue

            # Billing_Read is the value the utility bills on; fall back to the LCD display
            # value if it is absent.
            value = self._parse_float(raw_read.billing_read)
            if value is None:
                value = self._parse_float(raw_read.lcd_read)

            register_value, register_unit = self.map_reading(
                value, self._map_unit(raw_read.unit)
            )
            meter_reads.append(
                GeneralMeterRead(
                    org_id=self.org_id,
                    device_id=meter_id,
                    account_id=raw_read.reference,
                    location_id=raw_read.reference,
                    flowtime=flowtime,
                    register_value=register_value,
                    register_unit=register_unit,
                    # The API provides cumulative reads only, no interval consumption.
                    interval_value=None,
                    interval_unit=None,
                    battery=None,
                    install_date=None,
                    connection=None,
                    estimated=None,
                )
            )

        return list(meters_by_id.values()), meter_reads

    def _parse_read_date(self, read_date: Optional[str]) -> Optional[datetime]:
        """
        Parse WaterScope's date-only Read_Date (e.g. "05/15/2018") into a timezone-aware
        datetime at midnight in the org's timezone. Falls back to ISO parsing in case the
        API ever returns an ISO timestamp.
        """
        if not read_date:
            return None
        try:
            parsed = datetime.strptime(read_date, self.DATE_FORMAT)
            return self.org_timezone.localize(parsed)
        except (TypeError, ValueError):
            pass
        # datetime_from_iso_str localizes naive datetimes with the org timezone for us.
        try:
            return self.datetime_from_iso_str(read_date, self.org_timezone)
        except (TypeError, ValueError):
            return None

    def _map_unit(self, unit: Optional[str]) -> Optional[str]:
        """
        Translate a WaterScope unit code to one of our canonical units. Unknown codes
        return None so the reading is kept unconverted rather than guessed at.
        """
        if unit is None:
            return None
        mapped = self.UNIT_MAP.get(unit.strip().upper())
        if mapped is None:
            logger.warning(f"Unrecognized WaterScope unit for {self.org_id}: {unit}")
        return mapped

    @staticmethod
    def _as_str(value) -> Optional[str]:
        return str(value) if value is not None else None

    @staticmethod
    def _parse_float(value) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _transform_meter_alerts(self, run_id, extract_outputs):
        """
        The WaterScope Billing API does not expose meter alerts, so there is nothing to
        transform.
        """
        return []


class MetronRawReadsLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "metron_read_base"

    def columns(self) -> List[str]:
        return list(MetronReading.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["meter_id", "read_date"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file("reads.json", MetronReading)
        return [tuple(getattr(i, name) for name in self.columns()) for i in raw_data]

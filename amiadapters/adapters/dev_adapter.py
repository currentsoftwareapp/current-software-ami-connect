from datetime import datetime, timedelta
import json
import logging
from typing import List, Tuple

from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.models import (
    GeneralMeter,
    GeneralMeterAlert,
    GeneralMeterRead,
    GeneralMeterUnitOfMeasure,
    GeneralModelJSONEncoder,
)
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader

logger = logging.getLogger(__name__)

METERS_FILE = "meters.json"
READS_FILE = "reads.json"

# Datetime-typed fields on the general models. Extract serializes datetimes to
# ISO strings, so transform parses these fields back into datetimes.
METER_DATETIME_FIELDS = ("meter_install_date",)
READ_DATETIME_FIELDS = ("flowtime", "install_date")

# The dev adapter loads nothing into raw tables — its data is already in the
# general format, so there's no provider-specific raw schema to store.
DEV_RAW_SNOWFLAKE_LOADER = RawSnowflakeLoader.with_table_loaders([])


class DevAdapter(BaseAMIAdapter):
    """
    Test-only adapter that hardcodes a small set of meters and reads instead of
    talking to a real AMI provider. The extract step emits data already in the
    general format, so the transform step just reads it back (parsing datetimes)
    and returns it. Useful for exercising the pipeline end to end — e.g. the
    post-processor and meter-alert logic.
    """

    # Dev devices
    DEVICES = [
        {
            "device_id": "63528547",
            "account_id": "1-1-0-1",
            "location_id": "1-1-0",
        },  # Christopher Koch
        {
            "device_id": "63536621",
            "account_id": "1-1-3-1",
            "location_id": "1-1-3",
        },  # Tiffany Bruce
    ]
    HOURLY_INTERVAL_VALUE = 500.0
    READ_UNIT = GeneralMeterUnitOfMeasure.CUBIC_FEET

    def __init__(
        self,
        org_id: str,
        org_timezone: str,
        pipeline_configuration,
        configured_task_output_controller,
        configured_meter_alerts,
        configured_metrics,
        configured_sinks,
    ):
        super().__init__(
            org_id,
            org_timezone,
            pipeline_configuration,
            configured_task_output_controller,
            configured_meter_alerts,
            configured_metrics,
            configured_sinks,
            DEV_RAW_SNOWFLAKE_LOADER,
        )

    def name(self) -> str:
        return f"dev-adapter-{self.org_id}"

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> ExtractOutput:
        meters = self._dev_meters()
        reads = self._dev_reads(extract_range_start, extract_range_end)
        logger.info(
            f"Generated {len(meters)} dev meters and {len(reads)} dev reads "
            f"for range {extract_range_start} to {extract_range_end}"
        )
        return ExtractOutput(
            {
                METERS_FILE: _to_json(meters),
                READS_FILE: _to_json(reads),
            }
        )

    def _dev_meters(self) -> List[GeneralMeter]:
        meters = []
        for device in self.DEVICES:
            meters.append(
                GeneralMeter(
                    org_id=self.org_id,
                    device_id=device["device_id"],
                    account_id=device["account_id"],
                    location_id=device["location_id"],
                    meter_id=device["device_id"],
                    endpoint_id=device["device_id"],
                    meter_install_date=datetime(2020, 1, 1),
                    meter_size="5/8",
                    meter_manufacturer="DevCo",
                    multiplier=1.0,
                    location_address="123 Test St",
                    location_city="Testville",
                    location_state="CA",
                    location_zip="90000",
                )
            )
        return meters

    def _dev_reads(
        self, extract_range_start: datetime, extract_range_end: datetime
    ) -> List[GeneralMeterRead]:
        """
        One read per device per day across the extract range, so the reads land
        inside whatever window the post-processor queries.
        """
        reads = []
        base_register_read = 1000.0
        epoch = datetime(2020, 1, 1, tzinfo=extract_range_start.tzinfo)
        flowtime = extract_range_start
        while flowtime < extract_range_end:
            hours_since_epoch = int((flowtime - epoch).total_seconds() // 3600)
            for device in self.DEVICES:
                register_value = base_register_read + (hours_since_epoch * self.HOURLY_INTERVAL_VALUE)
                reads.append(
                    GeneralMeterRead(
                        org_id=self.org_id,
                        device_id=device["device_id"],
                        account_id=device["account_id"],
                        location_id=device["location_id"],
                        flowtime=flowtime,
                        register_value=register_value,
                        register_unit=self.READ_UNIT,
                        interval_value=self.HOURLY_INTERVAL_VALUE,
                        interval_unit=self.READ_UNIT,
                        battery=None,
                        install_date=None,
                        estimated=0,
                        connection=None,
                    )
                )
            flowtime += timedelta(hours=1)
        return reads

    def _transform(
        self, run_id: str, extract_outputs: ExtractOutput
    ) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        # Data is already in the general format; just read it back, converting
        # the ISO datetime strings back into datetimes.
        meters = [
            GeneralMeter(**_with_datetimes(d, METER_DATETIME_FIELDS))
            for d in extract_outputs.load_from_file(METERS_FILE, dict)
        ]
        reads = [
            GeneralMeterRead(**_with_datetimes(d, READ_DATETIME_FIELDS))
            for d in extract_outputs.load_from_file(READS_FILE, dict, allow_empty=True)
        ]
        return meters, reads

    def _transform_meter_alerts(
        self, run_id: str, extract_outputs: ExtractOutput
    ) -> List[GeneralMeterAlert]:
        # No provider-sourced alerts; alerts for this org come from the
        # post-processor (e.g. daily-high-usage detection).
        return []


def _to_json(items: List) -> str:
    return "\n".join(json.dumps(i, cls=GeneralModelJSONEncoder) for i in items)


def _with_datetimes(raw: dict, datetime_fields: Tuple[str, ...]) -> dict:
    parsed = dict(raw)
    for field in datetime_fields:
        value = parsed.get(field)
        parsed[field] = datetime.fromisoformat(value) if value else None
    return parsed

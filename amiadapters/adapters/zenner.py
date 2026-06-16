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
)
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader, RawSnowflakeTableLoader

logger = logging.getLogger(__name__)


# Zenner USA's StealthAMI API.
BASE_URL = "https://api.stealthami.com"


@dataclass
class ZennerMeter:
    """
    A meter record from GET /api/Meters/GetAllMeters.

    Documented fields: meter ID, meter type, flow capacity, inlet/outlet, unit of measure,
    multiplier rule display, truncation rule display, technology, read type code.
    """

    meter_id: str
    meter_type: str
    flow_capacity: str
    inlet_outlet: str
    unit_of_measure: str
    multiplier_rule_display: str
    truncation_rule_display: str
    technology: str
    read_type_code: str


@dataclass
class ZennerAccountMeter:
    """
    An account-to-meter association from GET /api/AccountMeter/GetAllAccountMeters.

    Documented fields: account ID, meter ID, node ID, link date (date the MIU was associated
    with the meter), expire date (date the account is set to be inactive), and a unique key
    (account ID + meter ID + MIU ID).
    """

    account_id: str
    meter_id: str
    node_id: str
    link_date: str
    expire_date: str
    unique_key: str


@dataclass
class ZennerAccount:
    """
    An account record from GET /api/Accounts/GetAllAccounts. Example payload:

        {
            "account_ID": "3-1226I-00",
            "address": {
                "address": "0 LORENCITA & GRAND",
                "city": "WEST COVINA",
                "state": "CA",
                "zipCode": "91791"
            },
            "cycle": "01",
            "firstReadDate": "2000-01-01T00:00:00",
            "name": "L.A. CO. DEPT OF PUB",
            "route": "S",
            "utilityType": "Water"
        }

    The nested address object is flattened into the address/city/state/zip_code fields.
    """

    account_id: str
    name: str
    address: str
    city: str
    state: str
    zip_code: str
    first_read_date: str
    utility_type: str
    cycle: str
    route: str


@dataclass
class ZennerNode:
    """
    An MIU/node record from GET /api/Nodes/GetAllNodes.

    Documented fields: serial number, MIU type, MIU subtype, register type, activation date,
    out of service date.
    """

    serial_number: str
    miu_type: str
    miu_subtype: str
    register_type: str
    activation_date: str
    out_of_service_date: str


@dataclass
class ZennerReading:
    """
    A meter reading from GET /api/Readings/GetAllReadings. Example payload:

        {
            "account_ID": "3-00534-00",
            "lastBillRead": 15771,
            "lastReadId": 28033,
            "meter_ID": "288844",
            "read": 15771,
            "read_Dttm": "2026-06-14T00:00:00",
            "serialNumber": 2002787,
            "water_Mag_Disp": 1,
            "water_TruncRule_Disp": 0
        }

    The reading does not carry a unit of measure; that comes from the meter (GetAllMeters).
    `last_read_id` is used as the pagination cursor (StartReadingID query parameter).
    """

    last_read_id: int
    account_id: str
    meter_id: str
    serial_number: str
    flowtime: str
    reading: float
    last_bill_read: float
    multiplier_display: float
    truncation_rule_display: float


class ZennerAdapter(BaseAMIAdapter):
    """
    AMI Adapter for Zenner USA's StealthAMI HTTP API.
    """

    # Number of readings to request per page from GetAllReadings.
    READS_PAGE_SIZE = 1000

    # The API never populates a meter's unit of measure, so we default to cubic feet.
    DEFAULT_UNIT_OF_MEASURE = "CF"

    def __init__(
        self,
        org_id: str,
        org_timezone: str,
        pipeline_configuration,
        utility: str,
        username: str,
        password: str,
        configured_task_output_controller,
        configured_meter_alerts,
        configured_metrics,
        configured_sinks,
    ):
        # `utility` is the StealthAMI utility identifier sent in the `utility` header,
        # e.g. "zennerapi.valencia_heights_ca.2017".
        self.utility = utility
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
                    ZennerRawMetersLoader(),
                    ZennerRawAccountMetersLoader(),
                    ZennerRawAccountsLoader(),
                    ZennerRawNodesLoader(),
                    ZennerRawReadsLoader(),
                ]
            ),
        )

    def name(self) -> str:
        return f"zenner-{self.org_id}"

    def _headers(self) -> Dict[str, str]:
        return {
            "UserName": self.username,
            "Password": self.password,
            "utility": self.utility,
        }

    def _get(self, path: str, params: Dict = None) -> list:
        """
        Issue a GET against the StealthAMI API and return the parsed JSON body.
        All endpoints we use return a top-level JSON array.
        """
        url = f"{BASE_URL}{path}"
        response = requests.get(url, headers=self._headers(), params=params)
        if response.status_code != 200:
            raise Exception(
                f"Non-200 response from {path}: {response.status_code} {response.text}"
            )
        return response.json()

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> ExtractOutput:
        logger.info(
            f"Extracting {self.org_id} data from {extract_range_start} to {extract_range_end}"
        )
        meters = self._extract_all_meters()
        account_meters = self._extract_all_account_meters()
        accounts = self._extract_all_accounts()
        nodes = self._extract_all_nodes()
        reads = self._extract_all_reads(extract_range_start, extract_range_end)

        return ExtractOutput(
            {
                "meters.json": self._to_jsonl(meters),
                "account_meters.json": self._to_jsonl(account_meters),
                "accounts.json": self._to_jsonl(accounts),
                "nodes.json": self._to_jsonl(nodes),
                "reads.json": self._to_jsonl(reads),
            }
        )

    @staticmethod
    def _to_jsonl(records: list) -> str:
        return "\n".join(json.dumps(r, cls=DataclassJSONEncoder) for r in records)

    def _extract_all_meters(self) -> List[ZennerMeter]:
        raw_meters = self._get("/api/Meters/GetAllMeters")
        meters = [
            ZennerMeter(
                meter_id=(
                    str(m.get("meterID")) if m.get("meterID") is not None else None
                ),
                meter_type=m.get("meterType"),
                flow_capacity=m.get("flowCapacity"),
                inlet_outlet=m.get("inletOutlet"),
                unit_of_measure=m.get("unitOfMeasure"),
                multiplier_rule_display=m.get("multiplierRuleDisplay"),
                truncation_rule_display=m.get("truncationRuleDisplay"),
                technology=m.get("technology"),
                read_type_code=m.get("readTypeCode"),
            )
            for m in raw_meters
        ]
        logger.info(f"Extracted {len(meters)} meters for {self.org_id}")
        return meters

    def _extract_all_account_meters(self) -> List[ZennerAccountMeter]:
        raw = self._get("/api/AccountMeter/GetAllAccountMeters")
        account_meters = []
        for am in raw:
            account_id = (
                str(am.get("accountID")) if am.get("accountID") is not None else None
            )
            meter_id = str(am.get("meterID")) if am.get("meterID") is not None else None
            node_id = str(am.get("nodeID")) if am.get("nodeID") is not None else None
            account_meters.append(
                ZennerAccountMeter(
                    account_id=account_id,
                    meter_id=meter_id,
                    node_id=node_id,
                    link_date=am.get("linkDate"),
                    expire_date=am.get("expireDate"),
                    # The API has no unique key field; build one from account + meter + MIU,
                    # as the documentation describes.
                    unique_key=f"{account_id}-{meter_id}-{node_id}",
                )
            )
        logger.info(f"Extracted {len(account_meters)} account meters for {self.org_id}")
        return account_meters

    def _extract_all_accounts(self) -> List[ZennerAccount]:
        raw = self._get("/api/Accounts/GetAllAccounts")
        accounts = []
        for a in raw:
            address = a.get("address") or {}
            accounts.append(
                ZennerAccount(
                    account_id=str(a.get("account_ID")),
                    name=a.get("name"),
                    address=address.get("address"),
                    city=address.get("city"),
                    state=address.get("state"),
                    zip_code=address.get("zipCode"),
                    first_read_date=a.get("firstReadDate"),
                    utility_type=a.get("utilityType"),
                    cycle=a.get("cycle"),
                    route=a.get("route"),
                )
            )
        logger.info(f"Extracted {len(accounts)} accounts for {self.org_id}")
        return accounts

    def _extract_all_nodes(self) -> List[ZennerNode]:
        raw = self._get("/api/Nodes/GetAllNodes")
        nodes = [
            ZennerNode(
                serial_number=str(n.get("serialNumber")),
                miu_type=n.get("miU_Type"),
                miu_subtype=n.get("miU_SubType"),
                register_type=n.get("regType"),
                activation_date=n.get("activationDate"),
                out_of_service_date=n.get("outOfServiceDate"),
            )
            for n in raw
        ]
        logger.info(f"Extracted {len(nodes)} nodes for {self.org_id}")
        return nodes

    def _extract_all_reads(
        self, extract_range_start: datetime, extract_range_end: datetime
    ) -> List[ZennerReading]:
        """
        GetAllReadings paginates with a cursor: StartReadingID + NumRecords. We page until a
        response returns fewer records than we asked for.
        """
        reads = []
        seen = set()
        # StartReadingID is an offset into the result set: it starts at 0 and advances by
        # the number of records already returned. (The API docs only show StartReadingID=0
        # and NumRecords and never describe how to page; offset paging matches the example.)
        offset = 0
        while True:
            params = {
                "StartDateTime": extract_range_start.date().isoformat(),
                "EndDateTime": extract_range_end.date().isoformat(),
                "NumRecords": self.READS_PAGE_SIZE,
                "StartReadingID": offset,
            }
            logger.info(f"Extracting reads for {self.org_id}, StartReadingID={offset}")
            batch = self._get("/api/Readings/GetAllReadings", params)
            if not batch:
                break

            new_count = 0
            for r in batch:
                reading = ZennerReading(
                    last_read_id=r.get("lastReadId"),
                    account_id=(
                        str(r.get("account_ID"))
                        if r.get("account_ID") is not None
                        else None
                    ),
                    meter_id=(
                        str(r.get("meter_ID"))
                        if r.get("meter_ID") is not None
                        else None
                    ),
                    serial_number=(
                        str(r.get("serialNumber"))
                        if r.get("serialNumber") is not None
                        else None
                    ),
                    flowtime=r.get("read_Dttm"),
                    reading=r.get("read"),
                    last_bill_read=r.get("lastBillRead"),
                    multiplier_display=r.get("water_Mag_Disp"),
                    truncation_rule_display=r.get("water_TruncRule_Disp"),
                )
                # Guard against an endpoint that ignores StartReadingID and re-returns the
                # same records: a reading is uniquely identified by meter + timestamp.
                key = (reading.meter_id, reading.flowtime)
                if key in seen:
                    continue
                seen.add(key)
                reads.append(reading)
                new_count += 1

            offset += len(batch)

            # No new records means the endpoint is repeating pages - stop to avoid an
            # infinite loop rather than silently paging forever.
            if new_count == 0:
                logger.warning(
                    f"GetAllReadings returned {len(batch)} records but none were new at "
                    f"StartReadingID={offset - len(batch)} for {self.org_id}; stopping pagination."
                )
                break

            if len(batch) < self.READS_PAGE_SIZE:
                break

        logger.info(f"Extracted {len(reads)} reads for {self.org_id}")
        return reads

    def _transform(self, run_id: str, extract_outputs: ExtractOutput):
        raw_meters = extract_outputs.load_from_file("meters.json", ZennerMeter)
        raw_account_meters = extract_outputs.load_from_file(
            "account_meters.json", ZennerAccountMeter
        )
        raw_accounts = extract_outputs.load_from_file("accounts.json", ZennerAccount)
        raw_reads = extract_outputs.load_from_file("reads.json", ZennerReading)
        return self._transform_meters_and_reads(
            raw_meters, raw_account_meters, raw_accounts, raw_reads
        )

    def _transform_meters_and_reads(
        self,
        raw_meters: List[ZennerMeter],
        raw_account_meters: List[ZennerAccountMeter],
        raw_accounts: List[ZennerAccount],
        raw_reads: List[ZennerReading],
    ) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        # Only water accounts are relevant to this pipeline.
        accounts_by_id = {
            a.account_id: a for a in raw_accounts if a.utility_type == "Water"
        }
        account_meter_by_meter_id = self._account_meter_by_meter_id(raw_account_meters)
        # The API never provides a unit of measure, so fall back to the default.
        unit_by_meter_id = {
            str(m.meter_id): m.unit_of_measure or self.DEFAULT_UNIT_OF_MEASURE
            for m in raw_meters
        }

        meters_by_id = {}
        for raw_meter in raw_meters:
            if raw_meter.meter_id is None:
                logger.warning(
                    f"Skipping meter with no meter_id for {self.org_id}: {raw_meter}"
                )
                continue
            meter_id = str(raw_meter.meter_id)
            account_meter = account_meter_by_meter_id.get(meter_id)
            account = (
                accounts_by_id.get(account_meter.account_id)
                if account_meter is not None
                else None
            )
            account_id = account_meter.account_id if account_meter else None
            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=meter_id,
                account_id=account_id,
                # We have no separate location identifier from the API; reuse the account
                # ID as the location ID, as we've done for other sources.
                location_id=account_id,
                meter_id=meter_id,
                # The MIU/node associated with the meter acts as the endpoint identifier.
                endpoint_id=account_meter.node_id if account_meter else None,
                meter_install_date=self.datetime_from_iso_str(
                    account_meter.link_date if account_meter else None,
                    self.org_timezone,
                ),
                meter_size=None,
                meter_manufacturer=None,
                multiplier=self._parse_multiplier(raw_meter.multiplier_rule_display),
                location_address=account.address if account else None,
                location_city=account.city if account else None,
                location_state=account.state if account else None,
                location_zip=account.zip_code if account else None,
            )
            meters_by_id[meter_id] = meter

        meter_reads = []
        for raw_read in raw_reads:
            if raw_read.meter_id is None or raw_read.flowtime is None:
                logger.warning(
                    f"Skipping read with missing meter_id or flowtime for {self.org_id}: {raw_read}"
                )
                continue
            meter_id = str(raw_read.meter_id)
            # The reading carries no unit of measure; use the meter's unit (defaulted).
            register_value, register_unit = self.map_reading(
                raw_read.reading,
                unit_by_meter_id.get(meter_id, self.DEFAULT_UNIT_OF_MEASURE),
            )
            meter_reads.append(
                GeneralMeterRead(
                    org_id=self.org_id,
                    device_id=meter_id,
                    account_id=raw_read.account_id,
                    # We have no separate location identifier; reuse the account ID
                    location_id=raw_read.account_id,
                    flowtime=self.datetime_from_iso_str(
                        raw_read.flowtime, self.org_timezone
                    ),
                    register_value=register_value,
                    register_unit=register_unit,
                    interval_value=None,
                    interval_unit=None,
                    battery=None,
                    install_date=None,
                    connection=None,
                    estimated=None,
                )
            )

        return list(meters_by_id.values()), meter_reads

    def _account_meter_by_meter_id(
        self, account_meters: List[ZennerAccountMeter]
    ) -> Dict[str, ZennerAccountMeter]:
        """
        Index account meters by meter ID. We assume each meter has exactly one account
        meter association; validate that and raise if it is ever violated, since a
        duplicate would mean we'd silently pick an arbitrary association.
        """
        result: Dict[str, ZennerAccountMeter] = {}
        for am in account_meters:
            meter_id = str(am.meter_id)
            if meter_id in result:
                raise ValueError(
                    f"Expected account meters to be unique by meter_id, but found "
                    f"multiple associations for meter_id={meter_id} "
                    f"({result[meter_id].unique_key} and {am.unique_key})"
                )
            result[meter_id] = am
        return result

    @staticmethod
    def _parse_multiplier(multiplier_rule_display: Optional[str]) -> Optional[float]:
        if multiplier_rule_display is None:
            return None
        try:
            return float(multiplier_rule_display)
        except (TypeError, ValueError):
            return None

    def _transform_meter_alerts(self, run_id, extract_outputs):
        """
        The StealthAMI API does not expose meter alerts, so there is nothing to transform.
        """
        return []


class ZennerRawMetersLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "zenner_meter_base"

    def columns(self) -> List[str]:
        return list(ZennerMeter.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["meter_id"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file("meters.json", ZennerMeter)
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class ZennerRawAccountMetersLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "zenner_account_meter_base"

    def columns(self) -> List[str]:
        return list(ZennerAccountMeter.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["unique_key"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file(
            "account_meters.json", ZennerAccountMeter
        )
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class ZennerRawAccountsLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "zenner_account_base"

    def columns(self) -> List[str]:
        return list(ZennerAccount.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["account_id"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file("accounts.json", ZennerAccount)
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class ZennerRawNodesLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "zenner_node_base"

    def columns(self) -> List[str]:
        return list(ZennerNode.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["serial_number"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file("nodes.json", ZennerNode)
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class ZennerRawReadsLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "zenner_read_base"

    def columns(self) -> List[str]:
        return list(ZennerReading.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["meter_id", "flowtime"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file("reads.json", ZennerReading)
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]

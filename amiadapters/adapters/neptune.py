from collections import Counter
from dataclasses import dataclass, replace
import datetime
import json
import logging
import math
import time
from typing import Dict, Generator, List, Optional

import pytz
from pytz.tzinfo import DstTzInfo
import requests

from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.models import (
    DataclassJSONEncoder,
    GeneralMeter,
    GeneralMeterRead,
    GeneralMeterUnitOfMeasure,
)
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader

logger = logging.getLogger(__name__)


@dataclass
class Consumption:
    """
    Neptune docs: This Web service returns hourly consumption history within a date range for a utility's
    endpoints. Neptune recommends pulling consumption data for at least the last three
    days if data is being pulled every day. This practice helps account for hour intervals that
    were previously missing since backfilled data may be available.
    """

    meter_number: str
    miu_id: str
    consumption_amount: str
    consumption_with_multiplier: str
    reading_date_utc: str


@dataclass
class Endpoint:
    """
    Neptune docs: This Web service returns a list and details of claimed endpoints for a utility. A claimed
    endpoint is a meter / MIU that is associated with an account and imported in
    Neptune 360.
    """

    site_id: str
    account_number: str
    premise_key: str
    miu_id: str
    register_id: str
    meter_number: str
    meter_type: str
    meter_size: str
    meter_manufacturer: str
    dials: str
    multiplier: str
    unit_of_measure: str
    cycle_route: str


@dataclass
class Reading:
    """
    Neptune docs: This Web service returns captured readings within a date range for a utility's claimed
    endpoints. It returns the latest reading within an hour.
    """

    meter_number: str
    miu_id: str
    site_id: str
    reading: str
    reading_datetime_utc: str
    consumption_state: str
    consumption_35_days: str
    reverse_flow_state: str
    no_flow_35_days: str
    register_change_out: str


class NeptuneAdapter(BaseAMIAdapter):
    """
    AMI Adapter that uses HTTP to retrieve data from the Neptune 360 API. To obtain credentials,
    login to https://www.neptune360.com/sdkaccess.
    """

    URI = "https://yx9sozm43k.execute-api.us-east-1.amazonaws.com"
    PAGINATION_LIMIT = 1000

    def __init__(
        self,
        org_id,
        org_timezone,
        pipeline_configuration,
        site_id: str,
        api_key: str,
        client_id: str,
        client_secret: str,
        configured_task_output_controller,
        configured_meter_alerts,
        configured_metrics,
        configured_sinks,
    ):
        self.site_id = site_id
        self.api_key = api_key
        self.client_id = client_id
        self.client_secret = client_secret
        self.cached_bearer_token = None
        super().__init__(
            org_id,
            org_timezone,
            pipeline_configuration,
            configured_task_output_controller,
            configured_meter_alerts,
            configured_metrics,
            configured_sinks,
            NeptuneRawSnowflakeLoader(),
        )

    def name(self) -> str:
        return f"neptune-{self.org_id}"

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ):
        endpoints = self._get_endpoints()
        consumption = self._get_consumption(extract_range_start, extract_range_end)
        readings = self._get_readings(extract_range_start, extract_range_end)

        return ExtractOutput(
            {
                "endpoints.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in endpoints
                ),
                "consumption.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in consumption
                ),
                "readings.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in readings
                ),
            }
        )

    def _get_endpoints(self) -> List[Endpoint]:
        """
        GET /v2/endpoints
        This Web service returns a list and details of claimed endpoints for a utility.
        A claimed endpoint is a meter / MIU that is associated with
        an account and imported in Neptune 360. V2 has cycle/route on output.

        {
            "endpoints": [
                {
                    "site_id": "1001",
                    "account_number": "AC123456",
                    "premise_key": "PK98765",
                    "miu_id": "87654321",
                    "register_id": "REG56789",
                    "meter_number": "12345678",
                    "meter_type": "Water",
                    "meter_size": "1.0",
                    "meter_manufacturer": "Sensus",
                    "dials": "4",
                    "multiplier": "2",
                    "unit_of_measure": "CF",
                    "cycle_route": "123",
                }
            ],
            "paging": {
                "page": 0,
                "limit": 0,
                "total": 0,
                "next": "n",
                "prev": "p",
                "self": "s"
            }
        }
        """
        endpoint = "/api/v2/endpoints"
        params = {"site_id": self.site_id}

        result = []
        for data in self._page_for_data(endpoint, params):
            for endpoint_json in data.get("endpoints", []):
                endpoint = Endpoint(**endpoint_json)
                result.append(endpoint)

        logger.info(f"Retrieved {len(result)} endpoints")
        return result

    def _get_consumption(
        self, extract_range_start: datetime, extract_range_end: datetime
    ) -> List[Consumption]:
        """
        Example consumption response: {
            "site_id": "1001",
            "endpoints": [
                {
                    "miu_id": "87654321",
                    "meter_number": "12345678",
                    "consumption_history": [
                        {
                            "reading_date": "2021-01-01 12:00:00",
                            "consumption": 1,
                            "consumption_with_multiplier": 10,
                        }
                    ]
                }
            ],
            "paging": {
                "page": 0,
                "limit": 0,
                "total": 0,
                "next": "n",
                "prev": "p",
                "self": "s"
            }
        }
        """
        endpoint = "/api/v1/consumption"
        params = {
            "site_id": self.site_id,
            "begin_date": extract_range_start.strftime("%Y-%m-%d"),
            "end_date": extract_range_end.strftime("%Y-%m-%d"),
            "actual_consumption": False,
        }

        result = []
        for data in self._page_for_data(endpoint, params):
            for endpoint_json in data.get("endpoints", []):
                meter_number = endpoint_json.get("meter_number")
                miu_id = endpoint_json.get("miu_id")
                for consumption_history in endpoint_json.get("consumption_history", []):
                    consumption = Consumption(
                        meter_number=meter_number,
                        miu_id=miu_id,
                        consumption_amount=consumption_history.get("consumption"),
                        consumption_with_multiplier=consumption_history.get(
                            "consumption_with_multiplier"
                        ),
                        reading_date_utc=consumption_history.get("reading_date"),
                    )
                    result.append(consumption)

        logger.info(f"Retrieved {len(result)} consumption readings")
        return result

    def _get_readings(
        self, extract_range_start: datetime, extract_range_end: datetime
    ) -> List[Reading]:
        """
        Example readings response: {
            "endpoints": [
                {
                    "site_id": "1001",
                    "miu_id": "87654321",
                    "meter_number": "12345678",
                    "readings": [
                        {
                            "reading": "1000",
                            "reading_datetime": "2021-01-01 12:00:00",
                            "consumption_state": 0,
                            "consumption_35_days": 0,
                            "reverse_flow_state": 0,
                            "no_flow_35_days": 0,
                        }
                    ],
                },
            ],
            "paging": {
                "page": 0,
                "limit": 0,
                "total": 0,
                "next": "n",
                "prev": "p",
                "self": "s"
            }
        }
        """
        endpoint = "/api/v2/readings"
        params = {
            "site_id": self.site_id,
            "begin_date": extract_range_start.strftime("%Y-%m-%d"),
            "end_date": extract_range_end.strftime("%Y-%m-%d"),
        }

        result = []
        for data in self._page_for_data(endpoint, params):
            site_id = data.get("site_id")
            for endpoint_json in data.get("endpoints", []):
                meter_number = endpoint_json.get("meter_number")
                miu_id = endpoint_json.get("miu_id")
                for reading_json in endpoint_json.get("readings", []):
                    reading = Reading(
                        meter_number=meter_number,
                        miu_id=miu_id,
                        site_id=site_id,
                        reading=reading_json.get("reading"),
                        reading_datetime_utc=reading_json.get("reading_datetime"),
                        consumption_state=reading_json.get("consumption_state"),
                        consumption_35_days=reading_json.get("consumption_35_days"),
                        reverse_flow_state=reading_json.get("reverse_flow_state"),
                        no_flow_35_days=reading_json.get("no_flow_35_days"),
                        register_change_out=reading_json.get("register_change_out"),
                    )
                    result.append(reading)
        logger.info(f"Retrieved {len(result)} readings")
        return result

    def _page_for_data(self, endpoint: str, params: dict) -> Generator:
        """
        For the given endpoint, paginate through API calls until all data points found. Yield response JSON as we go.
        """
        url = f"{self.URI}{endpoint}"
        continue_pagination = True
        i = 0
        while continue_pagination:
            i += 1
            headers = {
                "Authorization": f"Bearer {self._token()}",
                "x-api-key": self.api_key,
            }
            response = self._make_request(url, headers=headers, params=params)
            data = response.json()

            yield data

            if next_endpoint := data.get("paging", {}).get("next"):
                url = f"{self.URI}{next_endpoint}"
            else:
                continue_pagination = False

            if i > self.PAGINATION_LIMIT:
                raise Exception(f"Exceeded pagination limit for {endpoint}")

    def _token(self) -> str:
        """
        This method is used to get a new bearer token. Returns cached token if possible. Cached token may be expired.
        """
        if self.cached_bearer_token is not None:
            return self.cached_bearer_token
        endpoint = "/api/v1/token"
        headers = {
            "x-api-key": self.api_key,
            "client-id": self.client_id,
            "client-secret": self.client_secret,
        }
        url = f"{self.URI}{endpoint}"
        response = self._make_request(url, headers=headers)
        logger.info(f"Retrieving new bearer token from {url}")
        response.raise_for_status()
        self.cached_bearer_token = response.json()["AccessToken"]
        return self.cached_bearer_token

    def _reset_token(self) -> str:
        """
        This method is used to force retrieval of a new bearer token.
        """
        self.cached_bearer_token = None
        return self._token()

    def _make_request(
        self, url: str, headers: dict, params: dict = None
    ) -> requests.Response:
        try:
            response = requests.get(url, headers=headers, params=params)
            logging.info(f"HTTP GET request made to {url}")

            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if response.status_code == 401:
                logging.info("Bearer token expired. Getting a new one.")
                headers["Authorization"] = f"Bearer {self._reset_token()}"
                return self._make_request(url, headers, params)
            elif response.status_code >= 500:
                logging.warning(
                    f"Server error occurred: {e}. Waiting for 1 minutes before retrying one more time."
                )
                time.sleep(60)  # Wait for 1 minute
                return self._make_request(url, headers, params)  # Retry the query
            elif response.status_code == 429:
                logging.error(
                    f"Rate limit has been reached. Please try again later. Message: {response.text}"
                )
                raise e
            else:
                logging.error(
                    f"Error for endpoint {url} with code {response.status_code}: {response.text}"
                )
                raise e

    def _transform(self, run_id: str, extract_outputs: ExtractOutput):
        endpoints_by_device_id = self._endpoints_by_device_id(extract_outputs)
        consumption_by_device_id = self._consumption_by_device_id(extract_outputs)
        readings_by_device_id = self._readings_by_device_id(extract_outputs)

        meters_by_id = {}
        reads_by_device_and_time = {}

        for device_id, endpoints in endpoints_by_device_id.items():
            # Assume first endpoint is most recently created
            endpoint = self._pick_endpoint_and_log_duplicates(endpoints)
            account_id = endpoint.account_number
            location_id = self._location_id(endpoint.account_number)
            meter_id = endpoint.meter_number
            endpoint_id = endpoint.miu_id

            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account_id,
                location_id=location_id,
                meter_id=meter_id,
                endpoint_id=endpoint_id,
                meter_install_date=None,
                meter_size=self.map_meter_size(endpoint.meter_size),
                meter_manufacturer=endpoint.meter_manufacturer,
                multiplier=endpoint.multiplier,
                location_address=None,
                location_city=None,
                location_state=None,
                location_zip=None,
            )
            meters_by_id[device_id] = meter

            interval_reads = consumption_by_device_id.get(device_id, [])
            register_reads = readings_by_device_id.get(device_id, [])
            register_multiplier = self._register_multiplier(interval_reads)
            if register_multiplier is None and register_reads:
                logger.info(
                    f"No register multiplier for device {device_id}: no nonzero consumption in this extract. Its register values are set to None."
                )

            # Create a record for every interval read
            seen_interval_reads = set()
            for raw_interval_read in interval_reads:
                flowtime = self.datetime_from_iso_str(
                    raw_interval_read.reading_date_utc, pytz.UTC
                )

                if flowtime in seen_interval_reads:
                    logger.info(
                        f"Dropping duplicate interval read {raw_interval_read} which matches {reads_by_device_and_time[(device_id, flowtime)]}"
                    )
                    continue

                amount = None
                try:
                    # consumption_amount is a raw register count;
                    # consumption_with_multiplier is the same usage in the meter's unit of measure
                    amount = float(raw_interval_read.consumption_with_multiplier)
                except (ValueError, TypeError):
                    logger.info(
                        f"Invalid interval read: {raw_interval_read}. Value set to None."
                    )

                interval_value, interval_unit = self.map_reading(
                    amount,
                    # Almost always "CF", sometimes null
                    (
                        endpoint.unit_of_measure
                        if endpoint.unit_of_measure
                        else GeneralMeterUnitOfMeasure.CUBIC_FEET
                    ),
                )

                read = GeneralMeterRead(
                    org_id=self.org_id,
                    device_id=device_id,
                    account_id=account_id,
                    location_id=location_id,
                    flowtime=flowtime,
                    register_value=None,
                    register_unit=None,
                    interval_value=interval_value,
                    interval_unit=interval_unit,
                    battery=None,
                    install_date=None,
                    connection=None,
                    estimated=None,
                )
                reads_by_device_and_time[(device_id, flowtime)] = read
                seen_interval_reads.add(flowtime)

            # For every register read, join it to the matching record. If no match,
            # create a new record.
            seen_register_reads = set()
            for raw_register_read in register_reads:
                flowtime = self.datetime_from_iso_str(
                    raw_register_read.reading_datetime_utc, pytz.UTC
                )

                if flowtime in seen_register_reads:
                    logger.info(
                        f"Dropping duplicate register read {raw_register_read} which matches {reads_by_device_and_time[(device_id, flowtime)]}"
                    )
                    continue

                amount = None
                try:
                    amount = float(raw_register_read.reading)
                except (ValueError, TypeError):
                    logger.info(
                        f"Invalid register read: {raw_register_read}. Value set to None."
                    )
                if amount is not None:
                    amount = (
                        amount * register_multiplier
                        if register_multiplier is not None
                        else None
                    )

                register_value, register_unit = self.map_reading(
                    amount,
                    # Almost always "CF", sometimes null
                    (
                        endpoint.unit_of_measure
                        if endpoint.unit_of_measure
                        else GeneralMeterUnitOfMeasure.CUBIC_FEET
                    ),
                )
                if (device_id, flowtime) in reads_by_device_and_time:
                    # Join register read onto the interval read object
                    existing_read = reads_by_device_and_time[(device_id, flowtime)]
                    read = replace(
                        existing_read,
                        register_value=register_value,
                        register_unit=register_unit,
                    )
                else:
                    # Create a new one
                    read = GeneralMeterRead(
                        org_id=self.org_id,
                        device_id=device_id,
                        account_id=account_id,
                        location_id=location_id,
                        flowtime=flowtime,
                        register_value=register_value,
                        register_unit=register_unit,
                        interval_value=None,
                        interval_unit=None,
                        battery=None,
                        install_date=None,
                        connection=None,
                        estimated=None,
                    )
                reads_by_device_and_time[(device_id, flowtime)] = read
                seen_register_reads.add(flowtime)

        return list(meters_by_id.values()), list(reads_by_device_and_time.values())

    def _register_multiplier(
        self, interval_reads: List[Consumption]
    ) -> Optional[float]:
        """
        Register reads arrive as raw register counts. One count is 0.01, 0.1 or 1 of the meter's unit
        of measure, depending on the meter, and the endpoint's multiplier field is empty. The consumption
        feed carries each read both as a count (consumption_amount) and in the unit of measure
        (consumption_with_multiplier), so the multiplier is the power of ten closest to their ratio.
        Returns the most common multiplier across the device's consumption reads, or None if no read
        has both values nonzero.
        """
        multipliers = Counter()
        for read in interval_reads:
            try:
                count = float(read.consumption_amount)
                scaled = float(read.consumption_with_multiplier)
            except (ValueError, TypeError):
                continue
            if count == 0 or scaled == 0:
                continue
            multipliers[10.0 ** round(math.log10(abs(scaled / count)))] += 1
        if not multipliers:
            return None
        return multipliers.most_common(1)[0][0]

    def _location_id(self, account_number: str) -> str:
        """
        e.g. "1234-56-7" -> "1234-56"
        """
        if account_number is None:
            return None
        return "-".join(account_number.split("-")[:2])

    def _endpoints_by_device_id(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, List[Endpoint]]:
        """
        Map each meter number to the list of endpoints associated with it.
        """
        raw_endpoints = self._read_file(extract_outputs, "endpoints.json", Endpoint)
        # Build map
        result = {}
        for endpoint in raw_endpoints:
            device_id = self._device_id(endpoint.meter_number, endpoint.miu_id)
            if device_id not in result:
                result[device_id] = []
            result[device_id].append(endpoint)
        return result

    def _consumption_by_device_id(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, List[Consumption]]:
        raw_consumption = self._read_file(
            extract_outputs, "consumption.json", Consumption
        )
        result = {}
        for read in raw_consumption:
            device_id = self._device_id(read.meter_number, read.miu_id)
            if device_id not in result:
                result[device_id] = []
            result[device_id].append(read)
        return result

    def _readings_by_device_id(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, List[Reading]]:
        raw_reads = self._read_file(extract_outputs, "readings.json", Reading)
        result = {}
        for read in raw_reads:
            device_id = self._device_id(read.meter_number, read.miu_id)
            if device_id not in result:
                result[device_id] = []
            result[device_id].append(read)
        return result

    def _pick_endpoint_and_log_duplicates(
        self, potential_endpoints: List[Endpoint]
    ) -> Endpoint:
        """
        The API returns multiple endpoints per meter_number+miu_ud, which we believe are just duplicates
        in the data. This function tries to pick the best representation of the endpoint and logs the ones
        we ignore.

        :potential_endpoints: list of endpoints that share the same device_id but appeared in the API response multiple times
        """
        if len(potential_endpoints) == 1:
            return potential_endpoints[0]

        first_account_number = potential_endpoints[0].account_number
        # Default to the first endpoint
        picked_i = 0
        for i, endpoint in enumerate(potential_endpoints):
            if endpoint.account_number != first_account_number:
                # This would indicate that duplicate endpoints from the API are actually different endpoints, which breaks our assumption
                # so throw an error
                raise Exception(
                    f"Duplicate endpoints found with differing account_numbers, we may be dropping data: {potential_endpoints}"
                )
            # Most of the time, the duplicates are only different by their unit_of_measure. One is null and the other is populated. Pick the populated one.
            if endpoint.unit_of_measure:
                picked_i = i

        picked_endpoint = potential_endpoints[picked_i]
        dropped_endpoints = (
            potential_endpoints[:picked_i] + potential_endpoints[picked_i + 1 :]
        )

        logger.info(f"Dropping duplicate endpoints: {dropped_endpoints}")

        return picked_endpoint

    def _device_id(self, meter_number: str, miu_id: str) -> str:
        """
        Concatenate endpoint fields into a unique device_id.
        """
        return f"{meter_number}-{miu_id}"

    def _read_file(
        self, extract_outputs: ExtractOutput, file: str, raw_dataclass
    ) -> Generator:
        """
        Read a file's contents from extract stage output, create generator
        for each line of text
        """
        lines = extract_outputs.load_from_file(file, raw_dataclass, allow_empty=True)
        yield from lines

    def _transform_meter_alerts(self, run_id, extract_outputs):
        """
        Not implemented.
        """
        return []


class NeptuneRawSnowflakeLoader(RawSnowflakeLoader):

    def load(self, *args):
        self._load_raw_endpoints(*args)
        self._load_raw_consumption(*args)
        self._load_raw_readings(*args)

    def _load_raw_endpoints(
        self,
        run_id: str,
        org_id: str,
        org_timezone: DstTzInfo,
        extract_outputs: ExtractOutput,
        snowflake_conn,
    ) -> None:
        self._load_raw_data(
            run_id,
            org_id,
            org_timezone,
            extract_outputs,
            snowflake_conn,
            extract_output_filename="endpoints.json",
            raw_dataclass=Endpoint,
            table="NEPTUNE_360_ENDPOINTS_BASE",
            unique_by=["meter_number", "miu_id"],
        )

    def _load_raw_consumption(
        self,
        run_id: str,
        org_id: str,
        org_timezone: DstTzInfo,
        extract_outputs: ExtractOutput,
        snowflake_conn,
    ) -> None:
        self._load_raw_data(
            run_id,
            org_id,
            org_timezone,
            extract_outputs,
            snowflake_conn,
            extract_output_filename="consumption.json",
            raw_dataclass=Consumption,
            table="NEPTUNE_360_CONSUMPTION_BASE",
            unique_by=["meter_number", "miu_id", "reading_date_utc"],
        )

    def _load_raw_readings(
        self,
        run_id: str,
        org_id: str,
        org_timezone: DstTzInfo,
        extract_outputs: ExtractOutput,
        snowflake_conn,
    ) -> None:
        self._load_raw_data(
            run_id,
            org_id,
            org_timezone,
            extract_outputs,
            snowflake_conn,
            extract_output_filename="readings.json",
            raw_dataclass=Reading,
            table="NEPTUNE_360_READINGS_BASE",
            unique_by=["meter_number", "miu_id", "reading_datetime_utc"],
        )

    def _load_raw_data(
        self,
        run_id: str,
        org_id: str,
        org_timezone: DstTzInfo,
        extract_outputs: ExtractOutput,
        snowflake_conn,
        extract_output_filename: str,
        raw_dataclass,
        table: str,
        unique_by: List[str],
    ) -> None:
        """
        Extract raw data from intermediate outputs, then load into raw data table.

        extract_output_filename: name of file in extract_outputs that contains the raw data
        raw_dataclass: e.g. Meter, used to deserialize raw data and determine table column names
        table: name of raw data table in Snowflake
        unique_by: list of field names used with org_id to uniquely identify a row in the base table
        """
        raw_data = extract_outputs.load_from_file(
            extract_output_filename, raw_dataclass, allow_empty=True
        )
        if not raw_data:
            logger.info(
                f"No data found for {extract_output_filename}, skipping raw load"
            )
            return
        temp_table = f"temp_{table}"
        fields = [f.lower() for f in raw_dataclass.__dataclass_fields__.keys()]
        unique_by = [u.lower() for u in unique_by]
        self._create_temp_table(
            snowflake_conn,
            temp_table,
            table,
            fields,
            org_timezone,
            org_id,
            raw_data,
        )
        self._merge_from_temp_table(
            snowflake_conn,
            table,
            temp_table,
            fields,
            unique_by,
        )

    def _create_temp_table(
        self, snowflake_conn, temp_table, table, fields, org_timezone, org_id, raw_data
    ) -> None:
        """
        Insert every object in raw_data into a temp copy of the table.
        """
        logger.info(f"Prepping for raw load to table {table}")

        # Create the temp table
        create_temp_table_sql = (
            f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} LIKE {table};"
        )
        snowflake_conn.cursor().execute(create_temp_table_sql)

        # Insert raw data
        columns_as_comma_str = ", ".join(fields)
        qmarks = "?, " * (len(fields) - 1) + "?"
        insert_temp_data_sql = f"""
            INSERT INTO {temp_table} (org_id, created_time, {columns_as_comma_str}) 
                VALUES (?, ?, {qmarks})
        """
        created_time = datetime.datetime.now(tz=org_timezone)
        rows = [
            tuple(
                [org_id, created_time] + [i.__getattribute__(name) for name in fields]
            )
            for i in raw_data
        ]
        snowflake_conn.cursor().executemany(insert_temp_data_sql, rows)

    def _merge_from_temp_table(
        self,
        snowflake_conn,
        table: str,
        temp_table: str,
        fields: List[str],
        unique_by: List[str],
    ) -> None:
        """
        Merge data from temp table into the base table using the unique_by keys
        """
        logger.info(f"Merging {temp_table} into {table}")
        merge_sql = f"""
            MERGE INTO {table} AS target
            USING (
                -- Use GROUP BY to ensure there are no duplicate rows before merge
                SELECT 
                    org_id,
                    {", ".join(unique_by)},
                    {", ".join([f"max({name}) as {name}" for name in fields if name not in unique_by])}, 
                    max(created_time) as created_time
                FROM {temp_table} t
                GROUP BY org_id, {", ".join(unique_by)}
            ) AS source
            ON source.org_id = target.org_id 
                {" ".join(f"AND source.{i} = target.{i}" for i in unique_by)}
            WHEN MATCHED THEN
                UPDATE SET
                    target.created_time = source.created_time,
                    {",".join([f"target.{name} = source.{name}" for name in fields])}
            WHEN NOT MATCHED THEN
                INSERT (org_id, {", ".join(name for name in fields)}, created_time) 
                        VALUES (source.org_id, {", ".join(f"source.{name}" for name in fields)}, source.created_time)
        """
        snowflake_conn.cursor().execute(merge_sql)

CREATE TABLE NEPTUNE_360_ENDPOINTS_BASE (
    ORG_ID VARCHAR(16777216) NOT NULL,
    CREATED_TIME TIMESTAMP_TZ(9) NOT NULL,
    site_id VARCHAR(16777216),
    account_number VARCHAR(16777216),
    premise_key VARCHAR(16777216),
    miu_id VARCHAR(16777216),
    register_id VARCHAR(16777216),
    meter_number VARCHAR(16777216),
    meter_type VARCHAR(16777216),
    meter_size VARCHAR(16777216),
    meter_manufacturer VARCHAR(16777216),
    dials VARCHAR(16777216),
    multiplier VARCHAR(16777216),
    unit_of_measure VARCHAR(16777216),
    cycle_route VARCHAR(16777216),
    UNIQUE (ORG_ID, meter_number, miu_id)
);

CREATE TABLE NEPTUNE_360_CONSUMPTION_BASE (
    ORG_ID VARCHAR(16777216) NOT NULL,
    CREATED_TIME TIMESTAMP_TZ(9) NOT NULL,
    meter_number VARCHAR(16777216),
    miu_id VARCHAR(16777216),
    consumption_amount VARCHAR(16777216),
    consumption_with_multiplier VARCHAR(16777216),
    reading_date_utc VARCHAR(16777216),
    UNIQUE (ORG_ID, meter_number, miu_id, reading_date_utc)
);

CREATE TABLE NEPTUNE_360_READINGS_BASE (
    ORG_ID VARCHAR(16777216) NOT NULL,
    CREATED_TIME TIMESTAMP_TZ(9) NOT NULL,
    meter_number VARCHAR(16777216),
    miu_id VARCHAR(16777216),
    site_id VARCHAR(16777216),
    reading VARCHAR(16777216),
    reading_datetime_utc VARCHAR(16777216),
    consumption_state VARCHAR(16777216),
    consumption_35_days VARCHAR(16777216),
    reverse_flow_state VARCHAR(16777216),
    no_flow_35_days VARCHAR(16777216),
    register_change_out VARCHAR(16777216),
    UNIQUE (ORG_ID, meter_number, miu_id, reading_datetime_utc)
);

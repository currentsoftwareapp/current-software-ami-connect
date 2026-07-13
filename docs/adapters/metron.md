# Metron

The Metron adapter retrieves data via Metron Farnier's WaterScope Web API
(`https://webapi.waterscope.us`). API documentation is available at
`https://webapi.waterscope.us/Help`.

It pulls a billing read per meter from `GET /api/Billing`. Each record carries both the
meter identity and the account/location, so a single response is enough to build our
generalized meter and read models.

## Authentication

Requests are authenticated by passing `username` and `password` as query parameters.

## Configuration

The adapter needs no source-specific configuration beyond the base fields.

Example:
```
python cli.py config add-source my_utility metron America/Los_Angeles --sinks my_snowflake
```

## Secrets

Example:
```
python cli.py config update-secret my_utility --source-type metron --secret username=my_username --secret password=my_password
```

## Limitations

- The WaterScope API is billing-oriented and does **not** expose interval (hourly) reads.
  It returns a single cumulative register read per meter, with a date-only `Read_Date`, so
  this adapter produces daily/monthly billing reads only.
- The API does not expose meter alerts, so no alerts are produced.
- The API returns a single free-text address with no separate city/state/zip, and no
  MIU/radio (endpoint) identifier, meter size, install date, or multiplier.

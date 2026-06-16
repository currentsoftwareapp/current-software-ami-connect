# Zenner

The Zenner adapter retrieves data via Zenner USA's StealthAMI HTTP API
(`https://api.stealthami.com`).

It pulls meters, accounts, account-meter associations, and nodes (MIUs) for metadata, and
readings for the time series, then joins them into our generalized meter and read models.

## Authentication

Requests are authenticated with three HTTP headers: `UserName`, `Password`, and `utility`.
The `utility` value is the StealthAMI utility identifier, e.g.
`zennerapi.valencia_heights_ca.2017`.

## Configuration

- utility: StealthAMI utility identifier sent in the `utility` header

Example:
```
python cli.py config add-source my_utility zenner America/Los_Angeles --config utility=zennerapi.my_utility.1234 --sinks my_snowflake
```

## Secrets

Example:
```
python cli.py config update-secret my_utility --source-type zenner --secret username=my_username --secret password=my_password
```

## Limitations

- The StealthAMI API does not expose meter alerts, so no alerts are produced.
- The API returns a single combined address string per account; it is stored in
  `location_address` and the structured city/state/zip fields are left empty.
- The exact JSON keys for the API responses (and the `ReadingDTO` schema) are not published in
  the API documentation. The keys used in `amiadapters/adapters/zenner.py` are based on the
  documented field descriptions and should be verified against a live response.

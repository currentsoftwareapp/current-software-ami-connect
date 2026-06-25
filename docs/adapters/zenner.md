# Zenner

The Zenner adapter retrieves data via Zenner USA's StealthAMI HTTP API
(`https://api.stealthami.com`).

Documentation is available in a docx file shared offline, ask teammates for help.

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

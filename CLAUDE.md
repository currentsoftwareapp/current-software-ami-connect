# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

This is a Python 3.12 project. Work inside the virtualenv:

```bash
source venv/bin/activate          # create once: python3.12 -m venv venv && pip install -r requirements.txt
```

- **Run all tests** (from repo root): `python -m unittest` — use `unittest`, not pytest.
- **Run a single test**: `python -m unittest test.amiadapters.configuration.test_base` (module), or drill down to a case/method, e.g. `python -m unittest test.amiadapters.test_config.TestFromDatabase.test_from_database__builds_configuration`.
- **Format**: `black .` — CI enforces this (`.github/workflows/black.yml`), so run it before committing.
- **CLI**: `python cli.py --help`. It needs AWS credentials to reach Secrets Manager and Snowflake; set `AMI_CONNECT__AWS_PROFILE` (copy `.env.example` to `.env` and fill it in — `.env` is auto-loaded).

CI (`.github/workflows/build.yml`) runs `python -m unittest` on every push. Deploys run automatically when a commit lands on `main` (self-hosted EC2 runner); no local deploy setup is needed.

## Architecture

Three top-level packages:
- **`amiadapters/`** — standalone library that extracts data from AMI (water-meter) data sources and normalizes it. The bulk of the logic lives here.
- **`amicontrol/`** — Airflow 3.x control plane; DAGs that orchestrate the library in production.
- **`amideploy/`** — infrastructure-as-code for standing up the pipeline in AWS.

### Pipeline data flow

Each AMI data source (a "source" = utility/agency, keyed by `org_id`) is handled by an adapter subclassing `BaseAMIAdapter` (`amiadapters/adapters/base.py`). The pipeline runs these stages per adapter: **extract → transform → transform meter alerts → load_raw → load_transformed → post_process**.

- **Extract** should stay as close to source data as possible (it becomes the historical raw record). Its output is written via an **output controller** (`amiadapters/outputs/`: `local.py` for laptops, `s3.py` for production) as an "intermediate output" that later stages read back by `run_id`. Stages never pass Python objects to each other directly — they round-trip through these files precisely so each stage can be a **separate Airflow task in a different worker process**. Which controller is used is configuration-driven.
- **Transform** converts source data into the generalized frozen dataclasses in `amiadapters/models.py`: `GeneralMeter`, `GeneralMeterRead`, `GeneralMeterAlert`. These are the pipeline's canonical schema — everything downstream speaks these.
- **Load** writes into **storage sinks** (`amiadapters/storage/`). `SnowflakeStorageSink` (`storage/snowflake.py`) is the only real sink today; Snowflake is both the data warehouse and the store for non-secret pipeline configuration.

### Configuration and secrets (two sources of truth)

Configuration is split, and this split is core to the codebase:
- **Non-secret config** (sources, sinks, pipeline settings, backfills, notifications) lives in **Snowflake configuration tables** (`configuration_*`).
- **Secrets** (credentials) live in **AWS Secrets Manager**, under the `ami-connect/<type>/<name>` prefix, where `<type>` is `sources`, `sinks`, or `pipeline`. `get_secrets()` fetches everything under the prefix and unpacks the slash-delimited names into a nested dict.

There is a bootstrap dependency: reading the Snowflake config tables requires Snowflake credentials, which themselves come from Secrets Manager.

`AMIAdapterConfiguration` (`amiadapters/config.py`) ties it together and has two constructors:
- `from_yaml(config, secrets)` — for local development (`python cli.py run --local ...`).
- `from_database()` — production; reads Secrets Manager + Snowflake, and also merges per-org meter-alert thresholds from the Utility Billing app's Postgres DB.

The config *reading/writing* logic lives in `amiadapters/configuration/` (`base.py` = connection factories + CLI-facing functions, `database.py` = Snowflake SQL, `secrets.py` = Secrets Manager, `models.py` = config/secret dataclasses, `env.py` = process-global env settings).

### Source-type registry

`ConfiguredAMISourceTypes` (`amiadapters/configuration/models.py`) is the enum that maps a source-type string (e.g. `"aclara"`) to its config dataclass, secrets dataclass, and allowed sinks. Adding support for a new AMI provider touches three places:
1. A new `BaseAMIAdapter` subclass in `amiadapters/adapters/`.
2. A new entry in `ConfiguredAMISourceTypes` (plus its config/secrets dataclasses).
3. A `case` in `AMIAdapterConfiguration.adapters()` (`config.py`) that instantiates the adapter from parsed config.

`from_database()` deliberately **skips** source types the running code doesn't recognize, so a new source can be added to the production config table before the handling code is deployed.

### DAG generation

`amicontrol/dags/main.py` is the single Airflow entrypoint. It calls `AMIAdapterConfiguration.from_database()` **once** (config loads are expensive and Airflow refreshes DAGs frequently), then generates DAGs per configured adapter via `ami_control_dag_factory` (`meter_read_dags.py`): a manual-run DAG, one scheduled DAG per `ScheduledExtract`, and a DAG per configured backfill. It also builds data-quality-check and log-cleanup DAGs.

### Notes

- The **Neptune** adapter is not in this repo (open-source constraints); its private repo is cloned at deploy time and added to `sys.path` at runtime — see the `NEPTUNE` branch in `config.py`.
- Airflow is installed against a **local fork** of Apache's constraints file (`constraints-3.2.1.txt`) to patch CVEs. The `--constraint` line in `requirements.txt` must stay on its own line or pip ignores it.

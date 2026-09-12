# SaaS Metrics Pipeline (Apache Airflow)

![Python](https://img.shields.io/badge/python-3.11-blue)
![Airflow](https://img.shields.io/badge/airflow-2.10.4-017CEE)
![CI](https://github.com/YOUR_GITHUB_USER/saas-metrics-pipeline-airflow/actions/workflows/ci.yml/badge.svg)
![License](https://img.shields.io/badge/license-MIT-green)

A Medallion (Bronze/Silver/Gold) Apache Airflow pipeline that turns day-by-day SaaS billing and product-usage data into the metrics a growth/finance/product team actually looks at every morning: **MRR, ARR, New/Expansion/Contraction/Churned MRR, churn rate, NRR, ARPU, and DAU/WAU/MAU**.

---

## About this project

This is a personal portfolio project. All data is **100% synthetic**, generated locally by `include/saas/data_generator.py` — there is no real company, customer, or production system behind it, and no claim to the contrary should be read into anything below.

The input data represents what would normally arrive from an operational system somewhere in the business — whether as a file handed over by an internal team, or pulled directly from a database or external service. To keep the project reproducible without depending on credentials or third-party data, those origins are simulated locally by the generator, but the ingestion layer is built to be plugged into a real source without touching Silver or Gold.

It's an evolution of an earlier, simpler project of mine (a breweries-API pipeline) that used `PythonOperator`, Airflow 2.5 (long EOL), business logic embedded directly in the DAG file, and no automated quality gate. Every one of those has a deliberate fix here — see [What this demonstrates](#what-this-demonstrates).

---

## Architecture

```
   Synthetic data generator (billing + product usage)
              │
              ├──────────────┬───────────────────────────┐
              ▼              ▼                           │
      include/data/     source_db (Postgres)              │
      incoming/          "operational system"             │
      (file-drop origin)   (db-extract origin)             │
              │              │                            │
              └──────┬───────┘                            │
                      ▼                                    │
       ┌───────────────────────────┐                       │
       │  BRONZE  (bronze_ingestion DAG, @daily)            │
       │  ingest(source) → validate schema contract         │
       │  → include/data/bronze/dt=<date>/*.json            │
       └─────────────┬─────────────┘                       │
                      │ Dataset: saas://bronze/daily         │
                      ▼                                     │
       ┌───────────────────────────┐                        │
       │  SILVER  (silver_transformation DAG)                │
       │  normalize · dedupe · reconcile                     │
       │  → include/data/silver/<entity>/dt=<date>/*.parquet │
       └─────────────┬─────────────┘                        │
                      │ Dataset: saas://silver/daily          │
                      ▼                                       │
       ┌───────────────────────────┐                          │
       │  GOLD  (gold_metrics DAG)                             │
       │  MRR/ARR/churn/NRR + DAU/WAU/MAU                      │
       │  → include/data/gold/{saas_metrics_daily,             │
       │                        product_engagement_daily}      │
       └─────────────┬─────────────┘                          │
                      ▼                                        │
        notebooks/saas_metrics_review.ipynb ──────────────────-┘
        (MRR trend, waterfall, DAU/MAU, at-risk accounts)
```

Every layer's DAG follows the same shape: an **execução** task, a **validação** task, and a `notify_status` task (`trigger_rule="all_done"`) that reports the run's outcome — Slack if `SLACK_WEBHOOK_URL` is set, a structured log line otherwise.

---

## What this demonstrates

| Concept | In a real production setup | As implemented here |
|---|---|---|
| Data origin | A real billing platform (e.g. Stripe-style webhooks) + a real operational DB | Synthetic generator writing to a local folder and a local Postgres |
| Ingestion | Same `ingest(source)` contract, one more adapter | `file_drop` and `db_extract` adapters, pluggable |
| Orchestration | Airflow on Kubernetes/Celery across a cluster | `LocalExecutor`, single Postgres for metadata, single Postgres for the "source system" |
| Data-aware scheduling | Datasets across many teams' DAGs | 3 DAGs chained via Datasets (see the [Silver DAG's docstring](dags/silver_transformation_dag.py) for a documented limitation around historical backfills) |
| Storage | A managed lakehouse (S3/ADLS + Iceberg/Delta) | Local Parquet, partitioned by `dt=` |
| Data quality | Pandera or Great Expectations at scale, often with a dedicated quality-metadata store | Pandera schemas, in-process, no extra infrastructure (see [Data quality](#data-quality)) |
| Alerting | PagerDuty/Opsgenie + Slack | Slack webhook, structured-log fallback |

---

## Quick start

```bash
git clone https://github.com/YOUR_GITHUB_USER/saas-metrics-pipeline-airflow.git
cd saas-metrics-pipeline-airflow
cp .env.example .env
docker compose up --build
```

Once the containers are healthy:

```bash
# Populate ~90 days of synthetic history (writes to include/data/incoming and, if reachable, source_db)
python -m include.saas.data_generator --days 90
```

Then open **http://localhost:8080** (default login `airflow` / `airflow`, set in `docker-compose.yml`), unpause all three DAGs, and trigger `bronze_ingestion` for the historical range:

```bash
docker compose exec airflow-scheduler airflow dags backfill bronze_ingestion -s <90-days-ago> -e <today>
```

Because `silver_transformation` and `gold_metrics` are Dataset-scheduled (not calendar-scheduled), a rapid historical backfill on `bronze_ingestion` won't automatically produce one Silver/Gold run per historical day — that's a real, documented property of Dataset-triggered scheduling, not a bug (see the docstring in [`dags/silver_transformation_dag.py`](dags/silver_transformation_dag.py)). Backfill those two explicitly right after:

```bash
docker compose exec airflow-scheduler airflow dags backfill silver_transformation -s <90-days-ago> -e <today>
docker compose exec airflow-scheduler airflow dags backfill gold_metrics -s <90-days-ago> -e <today>
```

From then on, the daily schedule + Dataset chain keeps everything moving one day at a time with no manual intervention.

Review the results in `notebooks/saas_metrics_review.ipynb`.

---

## Running without Docker

Airflow doesn't officially support Windows natively — use WSL2, Linux, or macOS for this path.

```bash
python -m venv .venv
source .venv/bin/activate
pip install "apache-airflow==2.10.4" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"
pip install --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt" -r requirements.txt

export AIRFLOW_HOME="$PWD/.airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=false
airflow db migrate
airflow users create --username airflow --password airflow --firstname Air --lastname Flow --role Admin --email admin@example.com

python -m include.saas.data_generator --days 90 --skip-db   # no source_db without Docker, file_drop still works
airflow standalone   # or run `airflow webserver` / `airflow scheduler` separately
```

With no `source_db`, keep the `ingestion_source` Airflow Variable set to `file_drop` (the default).

---

## Project structure

```
saas-metrics-pipeline-airflow/
├── dags/
│   ├── bronze_ingestion_dag.py       # @daily, catchup=True — the historical backfill
│   ├── silver_transformation_dag.py  # Dataset-scheduled on Bronze
│   └── gold_metrics_dag.py           # Dataset-scheduled on Silver, SLA on the MRR task
├── include/
│   ├── saas/
│   │   ├── data_generator.py     # synthetic source of truth (file-drop + db-extract)
│   │   ├── constants.py          # shared domain vocabulary (plans, statuses, ...)
│   │   ├── airflow_contracts.py  # Dataset URIs / Variable names shared across DAGs
│   │   ├── ingestion/
│   │   │   ├── base.py           # IngestionAdapter interface
│   │   │   ├── file_drop.py      # adapter: file dropped by the business
│   │   │   └── db_extract.py     # adapter: direct DB extraction via Airflow Connection
│   │   ├── transform.py          # Silver: normalize, dedupe, reconcile, persist
│   │   ├── metrics.py            # Gold: MRR waterfall, churn/NRR, DAU/WAU/MAU
│   │   └── quality.py            # Bronze contract + Pandera schemas + reconciliation
│   ├── notifications/slack.py    # on_failure_callback + notify_pipeline_status
│   └── data/                     # (gitignored) generated/incoming/bronze/silver/gold/quality_reports
├── notebooks/saas_metrics_review.ipynb
├── tests/
│   ├── unit/            # generator, transform, metrics (hand-computed MRR waterfall), ingestion, slack
│   ├── data_quality/    # Bronze contract + Pandera critical/tolerable split + reconciliation threshold
│   └── dags/            # DagBag import-clean, no cycles, retries, callbacks (needs Airflow installed)
├── docker-compose.yml   # LocalExecutor + 2 Postgres (Airflow metadata, source_db)
├── Dockerfile
└── requirements*.txt / pyproject.toml / .pre-commit-config.yaml
```

---

## Layer by layer

### Bronze — source-agnostic ingestion

`include/saas/ingestion/__init__.py` exposes a single `ingest(source, execution_date)`. `source` is `"file_drop"` or `"db_extract"`, read from the `ingestion_source` Airflow Variable — the DAG code never hardcodes which one is active. Both adapters produce the exact same shape (see `include/saas/ingestion/base.py`), so everything downstream is indifferent to where the data came from.

`customers` and `subscriptions` are emitted as a **full daily snapshot** (cheap at this volume, and it avoids needing change-data-capture/merge logic to answer "what did this table look like on day D"); `subscription_events`, `invoices`, and `usage_events` are naturally append-only, so they're emitted as **deltas** — only that day's activity.

### Silver — clean, dedupe, reconcile

`include/saas/transform.py` normalizes types (dates → `datetime64[ns]`, money → rounded `float`), dedupes on each entity's primary key, and runs the Pandera schema gate (`include/saas/quality.py`). A failure on an identifier column (`customer_id`, `subscription_id`, ...) is **critical** and fails the task; anything else (a stray null/negative MRR value, a bad enum) is **quarantined** — dropped from the usable table, recorded in `include/data/quality_reports/<date>.json`, and the run continues.

Two cross-entity checks run here too: every `paid` invoice should belong to a subscription currently `active`/`past_due` (`reconcile_invoices_vs_subscriptions`), and every usage event should reference a known `customer_id` (`find_orphan_usage_events`). The data generator deliberately injects a small trickle of both, specifically so this check has something real to catch.

### Gold — the metrics

**Worked example**: a customer on the Growth plan ($350/mo) upgrades to Scale ($1,200/mo). The generator emits one `subscription_events` row: `event_type="upgraded", mrr_before=350, mrr_after=1200`. `compute_mrr_waterfall` (`include/saas/metrics.py`) picks that row up and adds `1200 - 350 = 850` to that day's `expansion_mrr` — nothing about the subscriptions table's *current* state is needed for this, which is exactly why the waterfall is computed from `subscription_events`, not from a snapshot diff: a snapshot diff can tell you MRR changed, but not *why* (new vs. expansion vs. reactivation), which is the whole point of the New/Expansion/Contraction/Churned breakdown.

`reactivated` events are folded into **New MRR** — a reactivation's MRR baseline at the start of the day was $0, the same starting point as a brand-new subscription; spec's four-bucket formula has no separate slot for it, and this is the simplest way to be consistent with that.

Customer/revenue churn rate and NRR all need a start-of-period baseline (yesterday's subscriptions snapshot) — on the very first day of history, with no D-1 snapshot, these come back as `None` rather than a misleading `0`.

`product_engagement_daily` adds DAU/WAU/MAU per account plus an **at-risk** flag: usage roughly halved over the last week *and* the subscription is currently `past_due` — a small example of reasoning across billing and product data together, not just mechanical aggregation. (The lookback window is 7 days here, not the 14 days the spec used as an example — `past_due` in this model only persists ~5 days on average before resolving, so a 14+14-day comparison would mostly compare two periods where the account was already active again. Tuned to actually detect something, and said so in the code.)

An end-to-end reconciliation check (`assert_mrr_reconciliation`) confirms Gold's `mrr_total` matches the sum of Silver's `active`/`past_due` subscriptions for the same day — this runs as its own Airflow task (`validate_gold_reconciliation`), independent of whatever the compute task saw in memory.

---

## Data quality

**Pandera**, not Great Expectations — schemas are plain Python objects colocated with the pandas/Parquet code that already does the rest of Silver; no separate suite/checkpoint/data-docs infrastructure to maintain solo. The trade-off given up is Great Expectations' richer auto-generated data documentation and profiling, which matters more at a scale (many tables, many teams) this project doesn't have.

Run the checks:

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest tests/unit tests/data_quality --cov=include --cov-report=term-missing
```

`tests/dags` needs a real Airflow install (`tests/dags` and the `dags/` package both need it importable) — see [Running without Docker](#running-without-docker) for the constraints-pinned install, or run it inside the container:

```bash
docker compose exec airflow-scheduler pytest tests/dags
```

---

## Observability / alerting

`include/notifications/slack.py::notify_failure` is wired as `on_failure_callback` in every DAG's `default_args`. If `SLACK_WEBHOOK_URL` is set, it posts there; otherwise it logs a structured `PIPELINE_FAILURE` line. It never raises — a broken notification channel is not a reason to fail (or further break) the pipeline reporting on it.

Every DAG also ends in a `notify_status` task (`trigger_rule="all_done"`) that inspects every task instance in the run and reports success/failure as a whole, not just whether the last task happened to be reached.

`gold_metrics`'s `compute_gold_metrics` task carries an `sla=timedelta(hours=3)` — it's literally the number the finance/growth team is waiting on every morning, which is as concrete a reason to attach an SLA as this project has.

---

## Tech stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.10.4, TaskFlow API, LocalExecutor |
| Language | Python 3.11 |
| Data quality | Pandera |
| Storage | Parquet (Silver/Gold), JSON (Bronze), partitioned by `dt=` |
| Databases | PostgreSQL 16 (Airflow metadata + simulated source system) |
| Synthetic data | Faker + a seeded custom simulator |
| Lint/format | ruff, black, isort, pre-commit |
| Tests | pytest, pytest-cov |
| CI | GitHub Actions |
| Containerization | Docker, Docker Compose |

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `AIRFLOW_UID` | `50000` | Host UID for mounted volumes (Linux) |
| `AIRFLOW__CORE__FERNET_KEY` | *(empty)* | Airflow's Fernet key; empty means Connections/Variables aren't encrypted at rest — fine for this local demo, not for production |
| `AIRFLOW_META_DB_USER` / `_PASSWORD` / `_NAME` | `airflow` | Airflow's own metadata database |
| `SOURCE_DB_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_NAME` | see `.env.example` | The simulated "operational system" database `db_extract.py` reads from |
| `SLACK_WEBHOOK_URL` | *(empty)* | Optional; falls back to structured logging when unset |
| `GENERATOR_DEFAULT_DAYS` | `90` | Default `--days` for the standalone generator CLI |
| `GENERATOR_SEED` | `42` | RNG seed for reproducible generation |
| `INGESTION_SOURCE` (→ `ingestion_source` Airflow Variable) | `file_drop` | `file_drop` or `db_extract` |
| `SCHEDULE_OVERRIDE` (→ `schedule_override` Airflow Variable) | *(empty → `@daily`)* | Overrides `bronze_ingestion`'s cron schedule without a code change |

Copy `.env.example` to `.env` before `docker compose up`.

---

## Roadmap / next steps

- Migrate to Airflow 3.x once its executor/task-execution-API rewrite has matured further (this project intentionally targets the last mature 2.x line for stability — see the Dockerfile).
- Replace local Parquet with a real analytical store (DuckDB or a managed warehouse) and add `dbt` for the Silver/Gold SQL, keeping Python for ingestion and the synthetic generator.
- `CeleryExecutor` (or `KubernetesExecutor`) if this ever needed to scale beyond one node.
- A lightweight Streamlit/Metabase dashboard on top of the Gold tables, instead of the notebook.
- Terraform for a real cloud deployment.
- Revisit the Dataset-vs-cron trade-off on `silver_transformation`/`gold_metrics` if Airflow's dataset-coalescing behavior around backfills changes in a future release.

---

## Troubleshooting

**`docker compose up` fails on `source_db` or `postgres` health checks** — give it another minute; Postgres containers take a few seconds to become ready on first boot, and `airflow-init` waits on both.

**`bronze_ingestion` fails with "No file-drop export found"** — the generator hasn't been run yet, or not for a wide enough date range. Run `python -m include.saas.data_generator --days N` first.

**`silver_transformation` / `gold_metrics` never trigger** — they're Dataset-scheduled, not cron-scheduled; they only run after their upstream DAG updates the corresponding Dataset. Check the Datasets tab in the Airflow UI, and see the backfill note in [Quick start](#quick-start).

**`db_extract` ingestion source fails** — confirm the `source_db` Airflow Connection exists (seeded from `AIRFLOW_CONN_SOURCE_DB` in `docker-compose.yml`) and that the generator has been run at least once without `--skip-db`.

**Pandera version mismatch / import errors locally** — install with the same Airflow constraints file used in `Dockerfile` and `.github/workflows/ci.yml`; installing `requirements.txt` unconstrained can silently pull an incompatible SQLAlchemy/pandas version.

---

## License

MIT — see [LICENSE](LICENSE).

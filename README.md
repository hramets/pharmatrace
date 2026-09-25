# PharmaTrace
 
A batch data engineering pipeline that ingests adverse event reports from the [OpenFDA API](https://open.fda.gov/apis/drug/event/), transforms them through a medallion architecture, and produces a structured analytical dataset for drug safety insights.
 
---
 
## What it does
 
The FDA receives millions of adverse event reports from patients, doctors, and manufacturers every year. Each report describes a drug (or combination of drugs) a patient was taking and what happened to them — hospitalization, disability, death, or other serious outcomes.
 
PharmaTrace ingests this data, cleans and normalizes it, and produces analytical tables that answer two core questions for any given drug:
 
- **Which drugs are most frequently co-reported in serious adverse events?**
- **How has the volume of adverse event reports changed over time?**
> **Important caveat:** OpenFDA adverse event reports are self-reported and not clinical evidence. A spike in reports may reflect increased drug popularity or media coverage rather than increased risk. The pipeline surfaces signal — not proof of causation.
 
---
 
## Architecture
 
```
OpenFDA API
    │
    ▼  Python ingestion script
    │  - Day-by-day pagination using Link header / search_after
    │  - Rate limiting, 429/500 retry logic, read timeout handling
    │  - Checkpoint recovery via last_load_state.json
    │  - API key loaded from environment variable
    ▼
data/bronze/  (raw JSON files on disk)
    │
    ▼  scripts/batch_manager.py
    │  - Picks next N unprocessed files from bronze/
    │  - Copies them to bronze_active/ for processing
    │  - Tracks progress via last_processed.json
    ▼
data/bronze_active/  (current batch)
    │
    ▼  dbt (10 models)
    │
    ├── Silver layer (DuckDB)
    │   ├── stg_events_raw          Flattens bronze JSON, tracks source_file
    │   ├── stg_event_drugs_raw     Unnests patient.drug[] array
    │   ├── stg_event_reactions_raw Unnests patient.reaction[] array
    │   ├── int_events              Cleaned events — dates, types, decoded fields
    │   ├── int_event_drugs         Cleaned drugs — normalized names, dosages
    │   └── int_event_reactions     Cleaned reactions — MedDRA terms, outcomes
    │
    └── Gold layer (DuckDB)
        ├── dim_drug_ingredient     One row per brand name / active substance pair
        ├── fact_event              One row per adverse event report
        ├── fact_event_drug         One row per drug per report
        └── fact_event_reaction     One row per reaction per report
    │
    ▼  scripts/cleanup_bronze_active.py
    │  - Deletes processed files from bronze_active/
    │  - Updates last_processed.json checkpoint
    │
    ▼  Loop back until bronze/ is empty
    │
    ▼  Apache Airflow DAG (orchestration)
       - Schedules and monitors the full pipeline
       - Two tasks: ingest_openfda_data >> process_all_batches
       - process_all_batches loops: move_batch → dbt run → cleanup
```
 
---
 
## Data model
 
### Why these tables?
 
A single OpenFDA adverse event report has a nested structure: one report → one patient → multiple drugs + multiple reactions. Flattening this into one table would either lose data (truncating drugs/reactions) or create misleading cartesian joins (implying drug A caused reaction B when the data doesn't support that).
 
PharmaTrace models this correctly as four separate tables:
 
| Table | Grain | Primary key |
|---|---|---|
| `gold.fact_event` | One row per report | `id` (surrogate) |
| `gold.fact_event_drug` | One row per drug per report | composite: `event_id + drug_id + drug_role + drug_start_date + dosage` |
| `gold.fact_event_reaction` | One row per reaction per report | composite: `event_id + reaction_name` |
| `gold.dim_drug_ingredient` | One row per brand name / substance pair | `id` (surrogate, stable integer) |
 
Drugs and reactions are **siblings** under the same report — not linked to each other — because the raw data does not tell us which specific drug caused which specific reaction.
 
### Key design decisions
 
**Drug name normalization** — `medicinalproduct` in the source is unstructured free text: "ADVIL 200MG", "advil", "Advil Liqui-Gel" all refer to the same product. Silver applies: lowercase → strip formulation suffixes (XR, ER, 200MG etc.) → remove special characters → collapse whitespace.
 
**Active substance kept as-is** — combination products list multiple ingredients separated by `\` (e.g. `CALCIUM CHLORIDE\DEXTROSE\SODIUM CHLORIDE`). The field is stored as a single normalized string rather than split into separate rows. Splitting was evaluated but introduced more ambiguity than value for this dataset — the combination itself is meaningful clinical context.
 
**Seriousness flags** — the source uses `1` for present and absent/NULL for not present. Silver normalises these to `1`/`0` booleans for all seven seriousness fields (`death`, `hospitalization`, `life_threatening`, `disabling`, `congenital_anomaly`, `other`, and the top-level `serious`).
 
**Incremental loading** — all six silver models are incremental. Raw models track `source_file` to avoid reprocessing bronze files already loaded. Intermediate models filter by `transmission_date` watermark or event ID presence to avoid reprocessing records already in the table.
 
**Batch processing** — DuckDB reads all files in `bronze_active/` on each dbt run. Batch size is configurable in `scripts/batch_manager.py` (`BATCH_SIZE = 50`). This lets the pipeline handle arbitrarily large historical loads without running out of memory — it processes N files, loads them to DuckDB, deletes them, and repeats.
 
---
 
## Stack
 
| Tool | Role | Why chosen |
|---|---|---|
| Python | API ingestion, batch management | Flexible, standard for data engineering |
| Apache Airflow | Pipeline orchestration | Built to learn open-source orchestration as an alternative to Fabric pipelines |
| DuckDB | Storage — all three layers | Built to learn a modern analytical database outside the Azure/Fabric managed lakehouse model |
| dbt (DuckDB adapter) | Silver and gold transformations | Built to learn industry-standard open-source transformation tooling outside the Microsoft Fabric ecosystem |
| Docker Compose | Local environment | Reproducible, shareable, one-command setup |
 
All tools are free and open source. No cloud services required.
 
---
 
## Project structure
 
```
pharmatrace/
├── dags/
│   └── pharmatrace_dag.py       # Airflow DAG definition
├── data/                        # Local data — gitignored
│   ├── bronze/                  # Raw JSON files from OpenFDA
│   ├── bronze_active/           # Current batch being processed by dbt
│   │   └── last_processed.json  # Checkpoint: last processed bronze file
│   └── pharmatrace.duckdb       # DuckDB database (bronze/silver/gold)
├── dbt/
│   └── pharmatrace/
│       ├── macros/
│       │   └── generate_schema_name.sql  # Overrides dbt default schema naming
│       ├── models/
│       │   ├── silver/
│       │   │   ├── stg_events_raw.sql
│       │   │   ├── stg_event_drugs_raw.sql
│       │   │   ├── stg_event_reactions_raw.sql
│       │   │   ├── int_events.sql
│       │   │   ├── int_event_drugs.sql
│       │   │   └── int_event_reactions.sql
│       │   └── gold/
│       │       ├── dim_drug_ingredient.sql
│       │       ├── fact_event.sql
│       │       ├── fact_event_drug.sql
│       │       └── fact_event_reaction.sql
│       ├── dbt_project.yml
│       └── profiles.yml
├── ingestion/
│   └── openfda.py               # OpenFDA ingestion script
├── scripts/
│   ├── batch_manager.py         # Moves N bronze files to bronze_active/
│   └── cleanup_bronze_active.py # Deletes processed files, updates checkpoint
├── config/                      # Airflow config (gitignored)
├── logs/                        # Airflow logs (gitignored)
├── docker-compose.yaml
├── Dockerfile
├── paths.py                     # Central path definitions for Python scripts
├── .env.example
├── .gitignore
└── requirements.txt
```
 
---
 
## Setup
 
### Prerequisites
 
- Docker Desktop with WSL2 backend (Windows) or Docker Engine (Linux/Mac)
- Python 3.10+ and pip
- An OpenFDA API key — free, request at [open.fda.gov](https://open.fda.gov/apis/authentication/)
### 1. Clone the repository
 
```bash
git clone https://github.com/your-username/pharmatrace.git
cd pharmatrace
```
 
### 2. Configure environment variables
 
```bash
cp .env.example .env
```
 
Edit `.env`:
 
```
AIRFLOW_UID=50000
OPENFDA_API_KEY=your_key_here
BRONZE_PATH=/opt/airflow/project/data/bronze_active
```
 
### 3. Build and start the stack
 
```bash
docker compose up airflow-init
docker compose up
```
 
Airflow UI: `http://localhost:8080` — default credentials: `airflow` / `airflow`
 
### 4. Trigger the pipeline
 
In the Airflow UI, find `pharmatrace_dag` and click the play button to trigger it manually.
 
The DAG runs two tasks in sequence:
 
1. **`ingest_openfda_data`** — fetches new adverse event reports from OpenFDA day by day and saves them as JSON files in `data/bronze/`
2. **`process_all_batches`** — loops until all bronze files are processed:
   - Moves `BATCH_SIZE` files from `bronze/` to `bronze_active/`
   - Runs `dbt run` to transform the batch through silver → gold
   - Deletes processed files from `bronze_active/` and updates the checkpoint
---
 
## Example queries
 
### Top drugs by adverse event volume
 
```sql
select
    d.active_substance,
    count(distinct fed.event_id) as total_reports,
    count(distinct case when fe.serious = 1 then fed.event_id end) as serious_reports,
    round(
        count(distinct case when fe.serious = 1 then fed.event_id end) * 100.0 /
        count(distinct fed.event_id), 1
    ) as serious_pct
from gold.fact_event_drug fed
join gold.dim_drug_ingredient d on fed.drug_id = d.id
join gold.fact_event fe on fed.event_id = fe.id
group by d.active_substance
order by total_reports desc
limit 20;
```
 
### Report volume over time
 
```sql
select
    date_trunc('month', transmission_date) as month,
    count(*) as report_count
from gold.fact_event
group by 1
order by 1;
```
 
### Co-reported drugs in serious events
 
```sql
select
    da.active_substance as drug_a,
    db.active_substance as drug_b,
    count(distinct a.event_id) as co_reported_events
from gold.fact_event_drug a
join gold.fact_event_drug b
    on a.event_id = b.event_id
    and a.drug_id < b.drug_id
join gold.fact_event fe on a.event_id = fe.id
join gold.dim_drug_ingredient da on a.drug_id = da.id
join gold.dim_drug_ingredient db on b.drug_id = db.id
where fe.serious = 1
group by 1, 2
order by co_reported_events desc
limit 20;
```
 
---
 
## Engineering highlights
 
**Incremental loading** — all dbt models are incremental. Raw models track `source_file` (the bronze filename) to skip files already loaded. Intermediate models use `transmission_date` watermarks or event ID filters. Gold models use composite `unique_key` configs for upsert semantics.
 
**Checkpoint recovery** — the ingestion script saves the current URL and page number to `last_load_state.json` after every successful API response. On restart it picks up exactly where it left off, even mid-day with hundreds of pages.
 
**Batch processing loop** — the Airflow DAG loops through all unprocessed bronze files in configurable batches rather than attempting to process everything in one dbt run. This keeps DuckDB's memory footprint bounded regardless of how much historical data exists.
 
**Drug name normalization** — a four-step regex pipeline (suffix stripping → special character removal → standalone number removal → whitespace collapsing) applied in silver before deduplication. Active substance combination strings (e.g. `CALCIUM CHLORIDE\DEXTROSE`) are kept as-is rather than split — the combination is clinically meaningful context.
 
**Correct many-to-many modelling** — drugs and reactions are stored as separate fact tables linked to events, not to each other. This reflects what the data actually says: that these drugs and these reactions appeared in the same report, not that drug X caused reaction Y.
 
**API resilience** — the ingestion script handles 429 rate limit errors (wait and retry), 500 server errors (retry up to 5 times), read timeouts, and the OpenFDA 25,000-record skip limit via `search_after` pagination from the Link response header.
 
---
 
## Data source
 
- **OpenFDA Drug Adverse Events API** — [open.fda.gov/apis/drug/event](https://open.fda.gov/apis/drug/event/)
- ~20 million reports available as of 2026
- Updated regularly by the FDA
- Free to use with API key (1,000 requests/minute limit)

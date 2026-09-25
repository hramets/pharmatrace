from pathlib import Path


PROJECT_ROOT = Path(__file__).parent

BRONZE_DATA = PROJECT_ROOT / 'data' /'bronze'
BRONZE_LAST_LOAD_STATE = BRONZE_DATA / 'last_load_state.json'
BRONZE_ACTIVE_BATCH_DATA = PROJECT_ROOT / 'data' / 'bronze_active'
BRONZE_LAST_PROCESSED_FILE = BRONZE_ACTIVE_BATCH_DATA / 'last_processed.json'

DUCKDB_PATH = PROJECT_ROOT / 'data' / 'pharmatrace.duckdb'
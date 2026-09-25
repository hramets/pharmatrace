from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from paths import BRONZE_DATA, BRONZE_ACTIVE_BATCH_DATA, BRONZE_LAST_PROCESSED_FILE
import json
import re


BATCH_SIZE = 50  # Number of files to process in each batch
NAME_RE = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})_(?P<page>\d+)\.(?P<ext>[^.]+)$")


def parse_file_name(file_name) -> tuple:
    m = re.match(NAME_RE, file_name)
    if not m:
        return None
    return (m.group("date"), int(m.group("page")), file_name)


def get_bronze_files() -> list[str]:
    """
    Returns a list of all files in the bronze data folder.
    """
    if not BRONZE_DATA.exists():
        return []
    return [f.name for f in BRONZE_DATA.iterdir() if f.is_file() and re.match(NAME_RE, f.name)]


def sort_files_by_date_and_page(files: list[str]) -> list[tuple]:
    """
    Sorts a list of files by date and page number.
    """
    parsed_files = [parse_file_name(f) for f in files]
    parsed_files = [f for f in parsed_files if f is not None]
    parsed_files.sort(key=lambda t: (t[0], t[1]))
    return [f[2] for f in parsed_files]


def get_last_processed_order_index(sorted_file_names: list[tuple], last_processed_file_name: str) -> int:
    """
    Returns the index of the last processed file in the sorted list of file names.
    If the last processed file is not found, returns -1.
    """
    if last_processed_file_name is None:
        return -1
    
    try:
        return sorted_file_names.index(last_processed_file_name)
    except ValueError:
        raise ValueError(f"Last processed file '{last_processed_file_name}' not found in the sorted list of files.")


def get_next_batch(sorted_file_names: list[str], last_processed_index: int) -> list[str]:
    return sorted_file_names[last_processed_index + 1:last_processed_index + 1 + BATCH_SIZE]


def copy_files_to_active_batch(next_files_batch: list[str]):
    """
    Copies the next batch of files to the bronze_active folder.
    """
    for file_name in next_files_batch:
        src = BRONZE_DATA / file_name
        dst = BRONZE_ACTIVE_BATCH_DATA / file_name
        dst.write_bytes(src.read_bytes())


def main():

    if BRONZE_LAST_PROCESSED_FILE.exists():
        with open(BRONZE_LAST_PROCESSED_FILE, 'r') as f:
            last_processed_name = json.load(f)['name']
    else:
        last_processed_name = None
        
    sorted_file_names = sort_files_by_date_and_page(get_bronze_files())
    last_processed_ord_index = get_last_processed_order_index(
        sorted_file_names,
        last_processed_name
    )
    next_files_batch: list[str] = get_next_batch(sorted_file_names, last_processed_ord_index)
    copy_files_to_active_batch(next_files_batch)
    

if __name__ == "__main__":
    main()


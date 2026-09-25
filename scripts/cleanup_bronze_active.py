import re
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from paths import BRONZE_ACTIVE_BATCH_DATA, BRONZE_LAST_PROCESSED_FILE
import json


NAME_RE = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})_(?P<page>\d+)\.(?P<ext>[^.]+)$")


def get_active_bronze_files() -> list[str]:
    """
    Returns a list of all files in the bronze data folder.
    """
    if not BRONZE_ACTIVE_BATCH_DATA.exists():
        raise FileNotFoundError(f"Active bronze batch data folder does not exist: {BRONZE_ACTIVE_BATCH_DATA}")
    return [f.name for f in BRONZE_ACTIVE_BATCH_DATA.iterdir() if f.is_file() and re.match(NAME_RE, f.name)]


def parse_file_name(file_name) -> tuple:
    m = re.match(NAME_RE, file_name)
    if not m:
        return None
    return (m.group("date"), int(m.group("page")), file_name)


def sort_files_by_date_and_page(files: list[str]) -> list[tuple]:
    """
    Sorts a list of files by date and page number.
    """
    parsed_files = [parse_file_name(f) for f in files]
    parsed_files = [f for f in parsed_files if f is not None]
    parsed_files.sort(key=lambda t: (t[0], t[1]))
    return [f[2] for f in parsed_files]


def delete_files_in_active_batch():
    """
    Deletes processed files in the bronze_active folder.
    """
    if BRONZE_ACTIVE_BATCH_DATA.exists():
        for f in BRONZE_ACTIVE_BATCH_DATA.iterdir():
            if f.is_file() and re.match(NAME_RE, f.name):
                f.unlink()


def update_last_processed_state(file_path, last_processed_file_name: str):
    """
    Updates the last processed file name in the last_processed.json file.
    """
    with open(BRONZE_LAST_PROCESSED_FILE, 'w') as f:
        json.dump({'name': last_processed_file_name}, f)


def main():
    # Get the list of files in the bronze_active folder
    active_files = get_active_bronze_files()
    if not active_files:
        print("No files found in the bronze_active folder.")
        return

    sorted_files = sort_files_by_date_and_page(active_files)
    delete_files_in_active_batch()
    update_last_processed_state(BRONZE_LAST_PROCESSED_FILE, sorted_files[-1])
    print("Cleanup of bronze_active folder completed.")


if __name__ == "__main__":
    main()

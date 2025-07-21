import re, os

def parse_meta(file_path: str):
    """
    'メッセ武蔵境/マイジャグラーV/slot_machine_data_2025-07-19.csv'
    → ('メッセ武蔵境','マイジャグラーV','2025-07-19')
    """
    parts = file_path.split("/")
    store   = parts[-3]
    machine = parts[-2]
    date = re.search(r"(\d{4}-\d{2}-\d{2})", parts[-1]).group(1)
    return store, machine, date

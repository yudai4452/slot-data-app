# src/parse_meta.py

import re, datetime as dt

def parse_meta(path: str):
    """
    'メッセ武蔵境/マイジャグラーV/slot_machine_data_2025-07-19.csv'
    → ('メッセ武蔵境','マイジャグラーV', datetime.date(2025,7,19))
    """
    parts = path.strip("/").split("/")
    store, machine = parts[-3], parts[-2]
    m = re.search(r"(\d{4}-\d{2}-\d{2})", parts[-1])
    date = dt.date.fromisoformat(m.group(1))
    return store, machine, date

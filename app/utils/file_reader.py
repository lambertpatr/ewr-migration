# app/utils/file_reader.py
import pandas as pd

def read_users_file(filename: str, file_obj) -> pd.DataFrame:
    if filename.lower().endswith(".csv"):
        df = pd.read_csv(file_obj)
    elif filename.lower().endswith(".xlsx"):
        df = pd.read_excel(file_obj)
    else:
        raise ValueError("Only CSV and XLSX supported")

    # normalize column names
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )

    # normalize values
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    return df


def read_lois_users_file(filename: str, file_obj) -> pd.DataFrame:
    """Backward-compatible wrapper for the LOIS users importer.

    Some modules import `read_lois_users_file`; provide a thin alias to the
    general `read_users_file` function so both names work.
    """
    return read_users_file(filename, file_obj)



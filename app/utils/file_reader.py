# app/utils/file_reader.py
import pandas as pd

def read_users_file(filename: str, file_obj) -> pd.DataFrame:
    """Reads a user-uploaded file (CSV or Excel) into a pandas DataFrame.

    This function is designed to be robust against common formatting issues in
    Excel files, such as leading empty rows or metadata before the actual
    header row. It intelligently finds the first non-empty row and treats it
    as the header.

    Args:
        filename: The original name of the uploaded file.
        file_obj: The file-like object to read from.

    Returns:
        A pandas DataFrame with normalized column names and string-stripped values.
    
    Raises:
        ValueError: If the file is not a CSV or XLSX, or if no data rows are found.
    """
    if filename.lower().endswith(".csv"):
        # Keep CSV reading simple for now, but could be enhanced if needed.
        df = pd.read_csv(file_obj)
    elif filename.lower().endswith((".xlsx", ".xls")):
        # For Excel, find the real header row by scanning for a row that contains
        # a known column name. This handles files that have a title, metadata, or
        # blank rows before the actual data header.
        # 1. Read the whole sheet without a header so we can inspect every row.
        #    Do NOT use dtype=str here — we need real NaN values so isnull() works.
        df_no_header = pd.read_excel(file_obj, header=None)

        # 2. Known anchor column names (lowercase). We look for a row where at least
        #    one cell matches one of these — that row is the real header.
        ANCHOR_COLS = {"apprefno", "shname", "application_number", "applicationnumber"}

        header_row_index = -1
        for i, row in df_no_header.iterrows():
            # Build a set of non-null, non-empty string values from this row.
            # Exclude "nan" / "none" strings that may appear after coercion.
            row_values = set()
            for v in row:
                if pd.notna(v):
                    s = str(v).strip().lower()
                    if s and s not in ("nan", "none"):
                        row_values.add(s)
            if row_values & ANCHOR_COLS:
                header_row_index = i
                break

        if header_row_index == -1:
            # Fall back: use the first row that has at least one non-null cell
            for i, row in df_no_header.iterrows():
                if row.notna().any():
                    header_row_index = i
                    break

        if header_row_index == -1:
            return pd.DataFrame()

        # 3. Use header= parameter directly — this tells pandas exactly which
        #    row index to treat as the header, no seek/skiprows needed.
        file_obj.seek(0)
        df = pd.read_excel(file_obj, header=header_row_index, dtype=str)
    else:
        raise ValueError("Only CSV and XLSX/XLS files are supported")

    if df.empty:
        return df

    # Normalize column names: convert to string, strip whitespace, lowercase, replace space/slash.
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )

    # Normalize all values to stripped strings.
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    return df


def read_lois_users_file(filename: str, file_obj) -> pd.DataFrame:
    """Backward-compatible wrapper for the LOIS users importer.

    Some modules import `read_lois_users_file`; provide a thin alias to the
    general `read_users_file` function so both names work.
    """
    return read_users_file(filename, file_obj)



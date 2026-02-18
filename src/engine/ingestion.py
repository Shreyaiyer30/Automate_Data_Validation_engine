import pandas as pd
import io
from typing import Tuple, Optional

def load_dataset(uploaded_file) -> pd.DataFrame:
    """
    Robustly loads a dataset from an uploaded file (CSV or Excel).
    Handles encoding detection and Excel sheet selection is assumed 
    to be handled by the caller or specialized here.
    """
    name = uploaded_file.name
    if name.endswith(".csv"):
        raw_bytes = uploaded_file.getvalue()
        for encoding in ("utf-8", "utf-16", "latin1"):
            try:
                return pd.read_csv(io.BytesIO(raw_bytes), encoding=encoding)
            except (UnicodeDecodeError, Exception):
                continue
        raise ValueError("Could not decode the CSV file. Try saving it as UTF-8.")
    elif name.endswith((".xlsx", ".xls")):
        # Note: Caller handles sheet selection if multiple sheets exist
        # This default uses the first sheet.
        return pd.read_excel(uploaded_file)
    else:
        raise ValueError(f"Unsupported file format: {name}")

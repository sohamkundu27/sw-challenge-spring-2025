import pandas as pd
from pandas import DataFrame


def load_data(file_path: str) -> dict[str, DataFrame]:
    """
    Load stock data from a CSV file and split it by symbol.

    :param file_path: Path to the CSV file
    :return: Dictionary with stock data for each symbol
    """
    df = pd.read_csv(file_path, parse_dates=['ts_event'])
    df = df.set_index('ts_event')
    df = df.sort_index()
    return {str(symbol): group for symbol, group in df.groupby('symbol')}

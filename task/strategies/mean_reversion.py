import pandas as pd
from pandas import DataFrame

from task.strategies.strategy import Strategy


class MeanReversionStrategy(Strategy):
    def __init__(self, window: int = 20, threshold: float = 0.05):
        """
        Initialize the mean reversion strategy with parameters.

        :param window: The window size for the moving average.
        :param threshold: The threshold for generating signals.
        """
        self.window = window
        self.threshold = threshold

    def generate_signals(self, df: DataFrame):
        """
        Generate trading signals based on mean reversion logic.

        :param df: DataFrame containing OHLCV data.
        :return: DataFrame with trading signals.
        """
        signals = pd.DataFrame(index=df.index)
        # (TASK 3) TODO: Implement mean reversion logic, generate buy/sell signals
        return signals

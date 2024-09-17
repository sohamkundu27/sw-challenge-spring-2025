import pandas as pd

from task.strategies.strategy import Strategy


class MomentumStrategy(Strategy):
    def __init__(self, short_window: int, long_window: int):
        """
        Initialize the momentum strategy with parameters.

        :param short_window: The window size for the short_window in days.
        :param long_window: The window size for the long_window in days.
        """
        self.short_window = short_window
        self.long_window = long_window

    def generate_signals(self, df: pd.Dataframe):
        """
        Generate trading signals based on your custom strategy.

        :param df: DataFrame containing OHLCV data.
        :return: DataFrame with trading signals.
        """
        signals = pd.DataFrame(index=df.index)
        # (TASK 3) TODO: Implement your custom strategy logic
        return signals

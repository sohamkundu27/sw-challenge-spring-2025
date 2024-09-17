from abc import abstractmethod, ABC

from pandas import DataFrame


class Strategy(ABC):
    @abstractmethod
    def generate_signals(self, df: DataFrame):
        """
        Generate trading signals based on the strategy.

        :param df: DataFrame containing OHLCV data.
        :return: DataFrame with trading signals.
        """
        pass
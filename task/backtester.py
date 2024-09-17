from typing import Dict

from matplotlib import pyplot as plt
from pandas import DataFrame

from performance_metrics import max_drawdown, sharpe_ratio
from task.strategies import Strategy


class Backtester:
    def __init__(self, market_data: Dict[str, DataFrame], strategy: Strategy):
        """
        Initialize the backtester with data and a strategy.

        :param market_data: A dictionary of DataFrames containing OHLCV data keyed by symbol.
        :param strategy: An instance of a strategy class.
        """
        self.market_data = market_data
        self.strategy = strategy
        self.results = {}

    def run_backtest(self):
        """
        Execute the backtest for each symbol using the specified strategy.
        """
        for symbol, df in self.market_data.items():
            print(f"Running backtest for {symbol}")
            signals = self.strategy.generate_signals(df)
            portfolio = self._simulate_trading(df, signals)
            self.results[symbol] = portfolio

    def _simulate_trading(self, df: DataFrame, signals: DataFrame) -> DataFrame:
        """
        Simulate trading based on generated signals.

        :param df: DataFrame containing OHLCV data.
        :param signals: DataFrame containing trading signals.
        :return: DataFrame with portfolio performance.
        """
        # (TASK 2) TODO: Implement trading simulation logic
        pass

    def evaluate_performance(self):
        """
        Evaluate performance metrics based on backtest results.
        """
        if not self.results:
            print("No backtest results to evaluate!")
            return

        for ticker, portfolio in self.results.items():
            print(f"Evaluating performance for {ticker}")
            returns = portfolio['Returns']
            portfolio_value = portfolio['Portfolio Value']
            sr = sharpe_ratio(returns)
            mdd = max_drawdown(portfolio_value)

            # (TASK 2) TODO: Calculate benchmark returns from market data
            # alpha_value = alpha(returns, benchmark_returns)

            print(f"Sharpe Ratio: {sr}, Max Drawdown: {mdd}")

    def plot_results(self):
        """
        Plot the results of the backtest.
        """
        if not self.results:
            print("No backtest results to plot!")
            return

        for symbol, portfolio in self.results.items():
            plt.figure(figsize=(12, 6))
            plt.plot(portfolio['Portfolio Value'], label='Portfolio Value')
            plt.title(f'Equity Curve for {symbol}')
            plt.xlabel('Date')
            plt.ylabel('Portfolio Value')
            plt.legend()
            plt.show()
            # Plot other relevant charts (e.g., drawdowns)

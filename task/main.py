from task.backtester import Backtester
from task.strategies.mean_reversion import MeanReversionStrategy
from task.strategies.momentum import MomentumStrategy
from task.utils import load_data

if __name__ == "__main__":
    file_path = "../data/basic-20230328-20240809.ohlcv-1d.csv"
    data = load_data(file_path)

    # Initialize strategies
    mean_reversion_strategy = MeanReversionStrategy(window=20, threshold=0.05)
    momentum_strategy = MomentumStrategy(short_window=5, long_window=20)

    # Backtest Mean Reversion Strategy
    backtester_mr = Backtester(data, mean_reversion_strategy)
    backtester_mr.run_backtest()
    backtester_mr.evaluate_performance()
    backtester_mr.plot_results()

    # Backtest Custom Strategy
    backtester_m = Backtester(data, momentum_strategy)
    backtester_m.run_backtest()
    backtester_m.evaluate_performance()
    backtester_m.plot_results()


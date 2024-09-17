import pandas as pd


def sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.0) -> float:
    """
    Calculate the Sharpe Ratio of a portfolio or investment.

    The Sharpe Ratio measures the excess return per unit of risk (standard deviation).

    :param returns: A Pandas Series of periodic returns.
    :param risk_free_rate: The risk-free rate of return for the same period.
    :return: The Sharpe Ratio as a float.
    """
    # (TASK 1) TODO: Implement Sharpe Ratio calculation
    pass


def max_drawdown(portfolio_values: pd.Series) -> float:
    """
    Calculate the Maximum Drawdown of a portfolio.

    Maximum Drawdown is the maximum observed loss from a peak to a trough before a new peak is attained.

    :param portfolio_values: A Pandas Series of cumulative portfolio values over time.
    :return: The Maximum Drawdown as a float (expressed as a positive value).
    """
    # (TASK 1) TODO: Implement Max Drawdown calculation
    pass


def alpha(
    portfolio_returns: pd.Series,
    benchmark_returns: pd.Series,
    risk_free_rate: float = 0.0
) -> float:
    """
    Calculate the Alpha of a portfolio.

    Alpha represents the excess return of a portfolio relative to the return predicted by the Capital Asset Pricing Model (CAPM).

    :param portfolio_returns: A Pandas Series of portfolio returns.
    :param benchmark_returns: A Pandas Series of benchmark (market) returns.
    :param risk_free_rate: The risk-free rate of return for the same period.
    :return: The Alpha as a float.
    """
    # (TASK 1) TODO: Implement Alpha calculation using CAPM
    pass

import polars as pl
from typing import Dict
from datetime import datetime

class GeneralTrader:
    """
    A class to represent a general trading strategy framework.
    """

    def __init__(self, df_ohlc: pl.DataFrame):
        """
        Initializes the GeneralTrader.

        Args:
            df_ohlc (pl.DataFrame): A Polars DataFrame containing OHLC (Open, High, Low, Close)
                                    and potentially other market data.
        """
        # self.df_ohlc = df_ohlc
        self.pre_filter : pl.Expr = None
        self.added_columns : Dict[str, pl.Expr] = {}
        self.df_ohlc : pl.DataFrame = df_ohlc


    def add_pre_filter(self, filter_expr: pl.Expr) -> "GeneralTrader":
        """
        Apply a filter expression to the internal OHLC DataFrame.

        Parameters
        ----------
        filter_expr : pl.Expr
            A Polars expression used to filter the DataFrame rows.

        Returns
        -------
        GeneralTrader
            Returns self to allow method chaining.
        """
        self.df_ohlc = self.df_ohlc.filter(filter_expr)
        return self

    def add_column(self, name: str, expr: pl.Expr) -> "GeneralTrader":
        """
        Register a new column expression to be added to the DataFrame later.

        Parameters
        ----------
        name : str
            The name of the new column.
        expr : pl.Expr
            A Polars expression defining the values of the new column.

        Returns
        -------
        GeneralTrader
            Returns self to allow method chaining.
        """
        self.added_columns[name] = expr
        return self

    def apply_added_columns(self) -> "GeneralTrader":
        """
        Apply all registered new column expressions to the internal DataFrame.

        If no columns are registered, this method does nothing.


        Returns
        -------
        GeneralTrader
            Returns self to allow method chaining.
        """
        if self.added_columns:
            try:
                self.df_ohlc = self.df_ohlc.with_columns(**self.added_columns)
            except Exception as e:
                print(f"Error during apply_added_columns(): {e}")
                raise
        return self



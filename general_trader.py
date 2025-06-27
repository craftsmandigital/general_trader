import polars as pl
# from polars import selectors as cs
from typing import Dict
# from datetime import datetime

class GeneralTrader:
    """
    A class to represent a general trading strategy framework.
    """

    def __init__(self,
                 df_ohlc: pl.DataFrame,
                 filter_expr: pl.Expr | None = None
                 ):
        """
        Initializes the GeneralTrader.

        Args:
            df_ohlc (pl.DataFrame): A Polars DataFrame containing OHLC (Open, High, Low, Close)
                                    and potentially other market data.
        """
        # self.df_ohlc = df_ohlc
        self.added_columns : Dict[str, pl.Expr] = {}
        self.df_ohlc : pl.DataFrame = df_ohlc
        self.df_ohlc_initial : pl.DataFrame = df_ohlc
        self.pre_filter = self.add_pre_filter(filter_expr)


    def add_pre_filter(self, filter_expr: pl.Expr | None) -> "GeneralTrader":
        """
        Apply a filter expression to the internal OHLC DataFrame.

        Parameters
        ----------
        filter_expr : pl.Expr
            A Polars expression used to filter the DataFrame rows.
            Here is an example of a typical filter date and ticker:
            (
                pl.col("date").is_between(
                datetime(2019, 5, 22), datetime(2024, 5, 22), closed="none"
                ) & (
                pl.col("ticker").is_in(["MSFT", "AAPL", "TSLA"])
                )
            )

        Returns
        -------
        GeneralTrader
            Returns self to allow method chaining.
        """
        if filter_expr is None:
            return self
        self.df_ohlc_initial = self.df_ohlc_initial.filter(filter_expr)
        self.df_ohlc = self.df_ohlc_initial
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
        # initial_columns :list[str] = self.df_ohlc.columns

        if self.added_columns:
            try:
                # 1. Group & compute
                df_group = (
                    self.df_ohlc
                    .group_by(
                        pl.col("ticker"), maintain_order=True
                    )
                    .agg(pl.all(), **self.added_columns)
                )

                # 2. Getting all collumns of List data type.
                list_cols = [c for c, t in df_group.schema.items() if isinstance(t, pl.List)]

                # 3. This will explode all columns that are of the List data type.
                self.df_ohlc = (
                    df_group
                    .explode(list_cols)
                )
            except Exception as e:
                print(f"Error during apply_added_columns(): {e}")
                raise
        return self



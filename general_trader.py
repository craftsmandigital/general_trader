import polars as pl
from polars import DataFrame, Expr
from pydantic import BaseModel, field_validator, ValidationError, ConfigDict
# Import Optional and Dict
from typing import Set, Any, Optional, Dict

# Define the expected schema for the DataFrame
REQUIRED_OHLC_COLUMNS: Set[str] = {
    'ticker',
    'date',
    'open',
    'high',
    'low',
    'close',
    'volume'
}

# Define the primary key columns

PRIMARY_KEY_COLUMNS: list[str] = ['ticker', 'date']

class GeneralTrader(BaseModel):
    """
    A Pydantic model representing a general trading setup.

    Attributes:
        df_ohlc (pl.DataFrame): A Polars DataFrame containing OHLCV data
                                with specific required columns.
        initialFilter (Optional[pl.Expr]): An optional Polars expression used
                                           for initial filtering of the data.
                                           Defaults to None.
    """

    # Configure Pydantic to allow arbitrary types like Polars DataFrame and Expr
    model_config = ConfigDict(arbitrary_types_allowed=True)

    df_ohlc: DataFrame
    initialFilter: Optional[Expr] = None

    @field_validator('df_ohlc')
    @classmethod
    def validate_ohlc_dataframe(cls, v: Any) -> DataFrame:
        """Validates the structure of the df_ohlc DataFrame."""
        if not isinstance(v, DataFrame):
            raise TypeError('df_ohlc must be a Polars DataFrame')
        actual_columns = set(v.columns)
        if not REQUIRED_OHLC_COLUMNS.issubset(actual_columns):
            missing = REQUIRED_OHLC_COLUMNS - actual_columns
            raise ValueError(
                f"df_ohlc DataFrame is missing required columns: {missing}"
            )
        return v

    @field_validator('initialFilter')
    @classmethod
    def validate_initial_filter(cls, v: Any) -> Optional[Expr]:
        """Validates that initialFilter is either a Polars Expression or None."""
        if v is None:
            return None
        if not isinstance(v, Expr):
            raise TypeError('initialFilter must be a Polars Expression or None')
        return v


# --- Method to Modify df_ohlc ---
    # Update return type hint to 'GeneralTrader' (using quotes for forward reference)
    def add_columns(self, column_definitions: Dict[str, pl.Expr]) -> 'GeneralTrader':
        """
        Adds new columns to the internal df_ohlc DataFrame in place.

        Args:
            column_definitions: A dictionary where keys are the names for the
                                new columns (str) and values are the Polars
                                expressions (pl.Expr) used to compute them.

        Returns:
            The GeneralTrader instance itself (self), allowing for method chaining.

        Raises:
            TypeError: If input validation fails.
            pl.exceptions.*: Polars exceptions during expression evaluation.
        """
        # --- Input Validation ---
        if not isinstance(column_definitions, dict):
            raise TypeError("column_definitions must be a dictionary.")
        if not all(isinstance(k, str) for k in column_definitions.keys()):
            raise TypeError("All keys in column_definitions must be strings.")
        if not all(isinstance(v, pl.Expr) for v in column_definitions.values()):
             raise TypeError("All values in column_definitions must be Polars Expressions.")

        # --- Core Logic: Modify self.df_ohlc ---
        try:
            self.df_ohlc = self.df_ohlc.with_columns(**column_definitions)
        except Exception as e:
            print(f"Error applying expressions with with_columns: {e}")
            raise

        # Return self to allow chaining
        return self


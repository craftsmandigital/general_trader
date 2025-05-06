import polars as pl
from polars import DataFrame, Expr
# import polars_talib as plta
from pydantic import (
    BaseModel,
    field_validator,
    model_validator, # Keep model_validator
    ValidationError,
    ConfigDict
)
from typing import Set, Any, Optional, List, Dict
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

    The internal df_ohlc DataFrame can be modified by methods like add_columns.

    Attributes:
        df_ohlc (pl.DataFrame): A Polars DataFrame containing OHLCV data

                                with specific required columns. This DataFrame
                                will be modified by methods like add_columns.
        initialFilter (Optional[pl.Expr]): An optional Polars expression used
                                           for initial filtering of the data.
                                           Defaults to None.
        ohlcColumnsToUse (Optional[List[str]]): An optional list of column names
                                                from df_ohlc to specifically track
                                                or use. If provided, all names
                                                must exist in df_ohlc. Defaults
                                                to None.
                                                ticker and date is always present.
                                                Return all collums if None
    """

    # Configure Pydantic to allow arbitrary types like Polars DataFrame and Expr
    model_config = ConfigDict(arbitrary_types_allowed=True)

    df_ohlc: DataFrame
    initialFilter: Optional[Expr] = None
    ohlcColumnsToUse: Optional[list[str]] = None # ticker and date is always present. Return all collums if None

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



    # --- Model Validator (runs after field validators) ---
    @model_validator(mode='after')
    def process_ohlc_columns_to_use(self) -> 'GeneralTrader':
        """
        1. Validates that columns in ohlcColumnsToUse exist in the input df_ohlc.
        2. If ohlcColumnsToUse is provided, subsets self.df_ohlc to include
           only 'ticker', 'date', and the specified columns.
        """
        columns_to_use = self.ohlcColumnsToUse
        # self.df_ohlc here is the *cloned* DataFrame from the field validator
        df = self.df_ohlc

        if columns_to_use is not None:
            # Step 1: Validate existence of specified columns in the original df
            actual_columns_set = set(df.columns)
            specified_columns_set = set(columns_to_use)

            if not specified_columns_set.issubset(actual_columns_set):
                invalid_columns = specified_columns_set - actual_columns_set
                raise ValueError(
                    f"Columns specified in 'ohlcColumnsToUse' do not exist "
                    f"in the provided df_ohlc: {invalid_columns}"
                )

            # Step 2: Perform the selection if validation passed
            # Define base columns that are always kept
            base_columns = {'ticker', 'date'}
            # Combine base columns with user-specified columns (set handles duplicates)
            final_columns_to_select_set = base_columns.union(specified_columns_set)
            # Convert back to list for Polars select
            final_columns_to_select_list = list(final_columns_to_select_set)

            # Reassign self.df_ohlc to the selected subset
            # This modifies the DataFrame stored in the instance
            self.df_ohlc = df.select(final_columns_to_select_list)

        # Must return self for 'after' model validators
        return self


    

# --- Method to Modify df_ohlc ---
    # Update return type hint to 'GeneralTrader' (using quotes for forward reference)
    def add_columns_and_prefilter( # Renamed method
        self,
        column_definitions: Optional[Dict[str, pl.Expr]] = None, # Optional
        filter_expr: Optional[pl.Expr] = None
    ) -> 'GeneralTrader':
        """
        Optionally adds new columns and/or filters the internal df_ohlc DataFrame.

        At least one of 'column_definitions' (if provided, must not be empty)
        or 'filter_expr' must be supplied.

        The filter is applied *after* new columns (if any) are added, allowing
        the filter expression to reference these newly created columns.

        Args:
            column_definitions: Optional dictionary of
                                {new_column_name: polars_expression}.
                                If None or empty, no columns are added.
            filter_expr: Optional Polars expression to filter the DataFrame.
                         If None, no filter is applied.

        Returns:
            The GeneralTrader instance (self) for method chaining.

        Raises:
            ValueError: If neither column_definitions (non-empty) nor
                        filter_expr are provided.
            TypeError: If input validation fails for parameters.
            pl.exceptions.*: Polars exceptions during expression evaluation or filtering.
        """
        # --- Constraint Check: At least one operation must be requested ---
        # A "column operation" exists if column_definitions is not None AND not an empty dict
        has_column_operation = bool(column_definitions) # True if not None and not empty
        has_filter_operation = filter_expr is not None

        if not has_column_operation and not has_filter_operation:
            raise ValueError(
                "At least one of 'column_definitions' (non-empty) or 'filter_expr' "
                "must be provided to add_columns_and_prefilter."
            )

        # --- Input Validation for provided parameters ---
        if column_definitions is not None: # If None, it's fine. If not None, validate its content.
            if not isinstance(column_definitions, dict):
                # This case should ideally not be hit if has_column_operation is true,
                # but good for robustness.
                raise TypeError("column_definitions must be a dictionary or None.")
            if not all(isinstance(k, str) for k in column_definitions.keys()):
                raise TypeError("All keys in column_definitions must be strings.")
            if not all(isinstance(v, pl.Expr) for v in column_definitions.values()):
                raise TypeError("All values in column_definitions must be Polars Expressions.")
            # If column_definitions is an empty dict, has_column_operation will be False.

        if filter_expr is not None and not isinstance(filter_expr, pl.Expr):
            raise TypeError("The 'filter_expr' parameter must be a Polars Expression or None.")

        # --- Core Logic ---
        try:
            # Step 1: Add new columns if definitions are provided and not empty
            if has_column_operation: # Relies on bool(column_definitions)
                self.df_ohlc = self.df_ohlc.with_columns(**column_definitions)

            # Step 2: Apply the filter if it was provided
            if has_filter_operation:
                self.df_ohlc = self.df_ohlc.filter(filter_expr)

        except Exception as e:
            print(f"Error during add_columns_and_prefilter (adding columns or filtering): {e}")
            raise

        return self 



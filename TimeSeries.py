import pandas as pd
import matplotlib.pyplot as plt
import missingno as msno
import logging

logger = logging.getLogger(__name__)

@pd.api.extensions.register_dataframe_accessor("ts")
class TimeSeriesAccessor:

    def __init__(self, pandas_obj):
        try:
            try:
                self._validate(pandas_obj)
            except ValueError as e:
                logger.warning("DataFrame not in timeseries format, attempting convesion")
                self._obj = TimeSeriesAccessor.transform_investing_historical(pandas_obj)
        except:
            message = "Conversion failed. Doublecheck your 'Date' column..."
            logger.error(message)
            raise ValueError(message)
        
    @staticmethod
    def _validate(obj) -> None:
        """
        Check if the indexes of all DataFrames have the pandas datetime format.
        Raises a ValueError if not.
        """
        if not pd.api.types.is_datetime64_any_dtype(obj.index):
            raise ValueError(f"Index is not in pandas datetime format.")

    def find_biggest_gap(self) -> None:
        """
        Find the biggest gap (consecutive days missing) in the DataFrame.

        Returns:
        --------
        int
            Number of consecutive days missing in the biggest gap.
        tuple
            Date range of the biggest gap (start date, end date).
        """
        consecutive_gaps = self._obj.index.to_series().diff().dt.days - 1
        biggest_gap = consecutive_gaps.max()

        # Find the date range of the biggest gap
        biggest_gap_start = self._obj.index.to_series().loc[
            consecutive_gaps.idxmax()]
        biggest_gap_end = self._obj.index.to_series().loc[
            consecutive_gaps.idxmax() + pd.DateOffset(days=biggest_gap)]
        date_range_biggest_gap = (biggest_gap_start, biggest_gap_end)

        return biggest_gap, date_range_biggest_gap

    def perform_eda(self) -> None:
        """
        Perform exploratory data analysis on each DataFrame.
        Report the start and end period of each data source.
        Use df.info() to show the number of nulls in each column.
        """

        print(f"Start Date: {self._obj.index.min()}")
        print(f"End Date: {self._obj.index.max()}")
        print("\nInfo:")
        print(self._obj.info())
        
        self._obj.hist(bins=10);
        plt.tight_layout()

        msno.matrix(self._obj)

        # Find the biggest gap in the data
        biggest_gap, date_range_biggest_gap = self.find_biggest_gap()
        print(f"\nBiggest Gap in Data:")
        print(f"Consecutive days missing: {biggest_gap}")
        print(f"Date Range of the Biggest Gap: {date_range_biggest_gap}")
        
        
    def fill_forward(self, business_days=True):
        """
        Fill forward missing values in the DataFrame.

        Parameters:
        -----------
        business_days : bool, optional (default=True)
            If False, fill forward for all days. If True, fill forward only for business days.

        Returns:
        --------
        pd.DataFrame
            DataFrame with missing values filled forward.
        """
        if not business_days:
            filled_df = self._obj.ffill()
        else:
            # Fill forward only for business days
            filled_df = self._obj.resample('B').ffill()

        return filled_df

    def remove_weekend_days(self):
        """
        Remove weekend days (Saturday and Sunday) from the DataFrame.

        Returns:
        --------
        pd.DataFrame
            DataFrame with weekend days removed.
        """
        # Remove rows where the day of the week is Saturday (5) or Sunday (6)
        df_no_weekends = self._obj[self._obj.index.to_series().dt.dayofweek < 5]

        return df_no_weekends
    
    
    @staticmethod
    def report_missing_days(self, business=True) -> None:
        """
        Report missing days in a DataFrame.
        Include value counts of weekdays for the missing days.

        Parameters:
        -----------
        business_days : bool, optional (default=True)
            If True, reports only for missing business days.

        Returns:
        --------
        None
        """
        # Ensure 'Date' is in the correct datetime format
        self._obj['Date'] = pd.to_datetime(self._obj.index)

        if business:
            freq = 'B'
        else:
            freq = 'D'
        # Create a date range of all working days in the DataFrame's date range
        all_days = pd.date_range(start=self._obj['Date'].min(), end=self._obj['Date'].max(), freq=freq)

        # Find missing working days
        missing_days = all_days.difference(self._obj['Date'])

        # Report missing working days and value counts of weekdays
        if not missing_days.empty:
            print("Missing days:")
            print(missing_days)

            # Calculate value counts of weekdays for missing working days
            missing_weekday_counts = pd.Series(missing_days.to_series().dt.day_name()).value_counts()

            print("\nValue counts of weekdays for missing days:")
            print(missing_weekday_counts)
        else:
            print("No missing days.")
            
    
    @staticmethod
    def transform_investing_historical(df: pd.DataFrame, format='%m/%d/%Y') -> pd.DataFrame:
        """
        Transform an Investing.com historical DataFrame.

        Parameters:
        -----------
        df : pd.DataFrame
            The DataFrame containing Investing.com historical data.

        Returns:
        --------
        pd.DataFrame
            Transformed DataFrame with adjusted date format, 'Change %' as float, and added 'Weekday' column.
        """
        assert 'Date' in df.columns, "The 'Date' column is missing."

        # Convert the 'Date' column to date format
        df['Date'] = pd.to_datetime(df['Date'], format=format)

        if "Change %" in df.columns:
            # Convert the 'Change' column to float (remove the % sign)
            if pd.api.types.is_string_dtype(df['Change %']):
                df['Change %'] = df['Change %'].str.rstrip('%').astype('float')

        return df.set_index('Date').sort_index()
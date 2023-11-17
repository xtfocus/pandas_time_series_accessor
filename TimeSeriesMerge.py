import pandas as pd
import missingno as msno
from typing import Dict, Union


class DataMerger:
    """
    A class for merging and analyzing multiple DataFrames.

    Parameters:
    -----------
    dataframes : dict
        A dictionary of DataFrames where keys are names and values are pandas DataFrames.

    Raises:
    -------
    ValueError:
        If the DataFrames have indexes with different formats.

    Attributes:
    -----------

    Methods:
    --------
    perform_eda():
        Perform exploratory data analysis on each DataFrame.

    merge_outer():
        Perform an outer join on all DataFrames.

    visualize_missing_data():
        Use missingno to visualize missing data in the merged DataFrame.

    merge_inner():
        Perform an inner join on all DataFrames and report key statistics.
        
    # Example Usage:
    dataframes_dict = {'df1': df1, 'df2': df2, 'df3': df3}
    merger = DataMerger(dataframes_dict)
    merger.perform_eda()
    merger.merge_outer()
    merger.visualize_missing_data()
    inner_result = merger.merge_inner()

    """

    def __init__(self, dataframes: Dict[str, pd.DataFrame]):
        self.dataframes = dataframes
        self._check_index_format()

    def _check_index_format(self) -> None:
        """
        Check if the indexes of all DataFrames have the pandas datetime format.
        Raises a ValueError if not.
        """
        for name, df in self.dataframes.items():
            if not pd.api.types.is_datetime64_any_dtype(df.index):
                raise ValueError(
                    f"Index of DataFrame {name} is not in pandas datetime format."
                )
                
            self.dataframes[name] = df.add_suffix(f'_{name}')

    def perform_eda(self) -> None:
        """
        Perform exploratory data analysis on each DataFrame.
        Report the start and end period of each data source.
        Use df.info() to show the number of nulls in each column.
        """
        for name, df in self.dataframes.items():
            print(f"\nExploratory Data Analysis for {name}:")
            print(f"Start Date: {df.index.min()}")
            print(f"End Date: {df.index.max()}")
            print("\nInfo:")
            print(df.info())


    def merge_outer(self) -> pd.DataFrame:
        """
        Perform an outer join on all DataFrames and store the result in self.merged_data.
        """
        merged_data = pd.DataFrame()
        for name, df in self.dataframes.items():
            merged_data = pd.merge(
                merged_data,
                df,
                left_index=True,
                right_index=True,
                how='outer',
            )

        return merged_data

    def merge_inner(self) -> Union[None, pd.DataFrame]:
        """
        Perform an inner join on all DataFrames.
        Report the number of rows in the final result, start and end date of the result.

        Returns:
        --------
        pd.DataFrame or None:
            The inner-joined DataFrame or None if the merged DataFrame is empty.
        """
        inner_merged_data = pd.concat(
            self.dataframes.values(),
            axis=1,
            join='inner',
            sort=True,
        )

        if not inner_merged_data.empty:
            print(f"\nInner Join Result:")
            print(f"Number of Rows: {len(inner_merged_data)}")
            print(f"Start Date: {inner_merged_data.index.min()}")
            print(f"End Date: {inner_merged_data.index.max()}")
            return inner_merged_data
        else:
            print("Inner Join Result is empty.")
            return None

    @staticmethod
    def visualize_missing_data(df) -> None:
        """
        Use missingno to visualize missing data in the DataFrame.
        """
        msno.matrix(df)

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
            df['Change %'] = df['Change %'].str.rstrip('%').astype('float')

        return df.set_index('Date')


class TimeSeriesPreprocessor:
    def __init__(self, name, df):
        self.name = name
        self.df = df.sort_index()  # Sort the DataFrame by index during initialization

    def perform_eda(self):
        """
        Perform Exploratory Data Analysis for the provided DataFrame.

        Returns:
        --------
        None
        """
        print(f"\nExploratory Data Analysis for {self.name}:")
        print(f"Start Date: {self.df.index.min()}")
        print(f"End Date: {self.df.index.max()}")
        print("\nInfo:")
        print(self.df.info())

        msno.matrix(self.df)

        # Find the biggest gap in the data
        biggest_gap, date_range_biggest_gap = self.find_biggest_gap()
        print(f"\nBiggest Gap in Data:")
        print(f"Consecutive days missing: {biggest_gap}")
        print(f"Date Range of the Biggest Gap: {date_range_biggest_gap}")

    def find_biggest_gap(self):
        """
        Find the biggest gap (consecutive days missing) in the DataFrame.

        Returns:
        --------
        int
            Number of consecutive days missing in the biggest gap.
        tuple
            Date range of the biggest gap (start date, end date).
        """
        consecutive_gaps = self.df.index.to_series().diff().dt.days - 1
        biggest_gap = consecutive_gaps.max()

        # Find the date range of the biggest gap
        biggest_gap_start = self.df.index.to_series().loc[consecutive_gaps.idxmax()]
        biggest_gap_end = self.df.index.to_series().loc[consecutive_gaps.idxmax() + pd.DateOffset(days=biggest_gap)]
        date_range_biggest_gap = (biggest_gap_start, biggest_gap_end)

        return biggest_gap, date_range_biggest_gap

    def fill_forward(self, all_days=True):
        """
        Fill forward missing values in the DataFrame.

        Parameters:
        -----------
        all_days : bool, optional (default=True)
            If True, fill forward for all days. If False, fill forward only for business days.

        Returns:
        --------
        pd.DataFrame
            DataFrame with missing values filled forward.
        """
        if all_days:
            filled_df = self.df.ffill()
        else:
            # Fill forward only for business days
            filled_df = self.df.resample('B').ffill()

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
        df_no_weekends = self.df[self.df.index.to_series().dt.dayofweek < 5]

        return df_no_weekends

    @staticmethod
    def report_missing_days(df: pd.DataFrame, business=True) -> None:
        """
        Report missing days in a DataFrame.
        Include value counts of weekdays for the missing days.

        Parameters:
        -----------
        df : pd.DataFrame
            The DataFrame with 'Date' as the index.

        Returns:
        --------
        None
        """
        # Ensure 'Date' is in the correct datetime format
        df['Date'] = pd.to_datetime(df.index)

        if business:
            freq = 'B'
        else:
            freq = 'D'
        # Create a date range of all working days in the DataFrame's date range
        all_days = pd.date_range(start=df['Date'].min(), end=df['Date'].max(), freq=freq)

        # Find missing working days
        missing_days = all_days.difference(df['Date'])

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
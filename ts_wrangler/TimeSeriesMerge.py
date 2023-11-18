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



import pandas as pd
from typing import List
import matplotlib.pyplot as plt
import missingno as msno
import logging

logger = logging.getLogger(__name__)

@pd.api.extensions.register_dataframe_accessor("ts")
class TimeSeriesAccessor:

    def __init__(self, pandas_obj: pd.DataFrame) -> None:
        """
        Initialize the TimeSeriesAccessor.

        Parameters:
        -----------
        pandas_obj : pd.DataFrame
            The DataFrame to be accessed.
        """
        try:
            try:
                self._validate(pandas_obj)
                self._obj = pandas_obj
            except ValueError as e:
                logger.warning("DataFrame not in timeseries format, attempting convesion using default format='%m/%d/%Y'")
                self._obj = TimeSeriesAccessor.transform_investing_historical(pandas_obj)
        except:
            message = "Conversion failed. Please set a 'Date' column of type pandas datetime as the index"
            logger.error(message)
            raise ValueError(message)
        
    @staticmethod
    def _validate(obj: pd.DataFrame) -> None:
        """
        Check if the indexes of all DataFrames have the pandas datetime format.
        Raises a ValueError if not.

        Parameters:
        -----------
        obj : pd.DataFrame
            The DataFrame to be validated.
        """
        if not pd.api.types.is_datetime64_any_dtype(obj.index):
            raise ValueError(f"Index is not in pandas datetime format.")


    def _find_gaps(self) -> pd.DataFrame:
        """
        Find normal and business gaps in data.

        Returns:
        --------
        pd.DataFrame
            DataFrame containing information about consecutive gaps.
        """
        consecutive_gaps = self._obj.index.to_series().diff().dt.days - 1
        
        consecutive_gaps = (pd.
                            DataFrame(consecutive_gaps).
                            rename(columns={'Date':'days_ago'}).
                            reset_index().
                            dropna().
                            query('days_ago > 0')).dropna()
        
        consecutive_gaps['days'] = consecutive_gaps.apply(lambda row: pd.date_range(end=row['Date'], periods=int(row['days_ago']), freq='D'), axis=1)
        
        consecutive_gaps['weekday'] = consecutive_gaps['days'].apply(lambda x: [i.strftime("%A") for i in x])
        consecutive_gaps['logic_mask'] = (consecutive_gaps['weekday']
                                          .apply(lambda x: [
                                              (i not in ['Sunday', 'Monday']) for i in x
                                          ]
                                                )
                                         )
        consecutive_gaps['business_days'] = consecutive_gaps.apply(lambda x: [value for value, condition in zip(x['days'], x['logic_mask']) if condition], axis=1)
        consecutive_gaps['business_weekday'] = consecutive_gaps['weekday'].apply(lambda x:[i for i in x if i not in ['Sunday', 'Monday']])
        consecutive_gaps['business_days_ago'] = consecutive_gaps['business_weekday'].apply(len)
        return consecutive_gaps
        
        
    def find_biggest_gaps(self, business: bool = False, k: int = 5) -> pd.DataFrame:
        """
        Find the biggest gap (consecutive days missing) in the DataFrame.

        Parameters:
        -----------
        business : bool, optional (default=False)
            If True, only report gaps caused by consecutive business working days.
        k : int, optional (default=5)
            Number of top gaps to report.

        Returns:
        --------
        pd.DataFrame
            DataFrame showing top k biggest gaps.
        """
        consecutive_gaps = self._find_gaps()
        if business:
            gaps = consecutive_gaps[['business_days', 'business_weekday']].rename(columns={'business_days':'days', 'business_weekday':'weekday'})
        else:
            gaps = consecutive_gaps[['days', 'weekday']]
        
        gaps = gaps.reset_index(drop=True)            
        gaps['length'] = gaps['days'].apply(len)
        gaps = gaps.sort_values("length", ascending=False)
        
        return gaps.head(k)            


    def perform_eda(self) -> None:
        """
        Perform exploratory data analysis on each DataFrame.
        Report the start and end period of each data source.
        Use df.info() to show the number of nulls in each column.
        """
        
        print("===OVERVIEW===")

        print(f"Start Date: {self._obj.index.min()}")
        print(f"End Date: {self._obj.index.max()}")
        print("\nInfo:")
        print(self._obj.info())
        
        self._obj.hist(bins=10)
        plt.tight_layout()

        print("===MISSING ROWS===")
        msno.matrix(self._obj)

        # Find the biggest gap in the data
        print("Business gaps:")
        print(self.find_biggest_gaps(business=True))
        
        print("All days gaps:")
        print(self.find_biggest_gaps(business=False))
        
        
    def fill_forward(self, business: bool = True) -> pd.DataFrame:
        """
        Fill forward missing values in the DataFrame.

        Parameters:
        -----------
        business : bool, optional (default=True)
            If False, fill forward for all days. If True, fill forward only for business days.

        Returns:
        --------
        pd.DataFrame
            DataFrame with missing values filled forward.
        """
        if not business:
            filled_df = self._obj.resample('D').ffill()
        else:
            # Fill forward only for business days
            filled_df = self._obj.resample('B').ffill()

        return filled_df

    def remove_weekend_days(self) -> pd.DataFrame:
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
    
    
    def report_missing_days(self, business=True) -> pd.Series:
        """
        Report missing days in a DataFrame.
        Include value counts of weekdays for the missing days.

        Parameters:
        -----------
        business: bool, optional (default=True)
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
            
        return (missing_days.to_series())
            
    
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
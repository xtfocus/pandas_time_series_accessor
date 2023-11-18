import pandas as pd
import pytest
from TimeSeries import TimeSeriesAccessor

# Fixture: Sample DataFrame with datetime index not named Date
@pytest.fixture
def sample_noname_dataframe() -> pd.DataFrame:
    """
    Fixture for creating a sample DataFrame with datetime index.
    """
    data = {'value': [1, 2, 3]}
    index = pd.date_range('2022-01-01', periods=3)
    return pd.DataFrame(data, index=index)

# Fixture: Sample DataFrame with datetime index named Date
@pytest.fixture
def sample_date_dataframe() -> pd.DataFrame:
    """
    Fixture for creating a sample DataFrame with datetime index.
    """
    data = {'value': [1, 2, 3]}
    index = pd.date_range('2022-01-01', periods=3)
    return pd.DataFrame(data, index=index).rename_axis('Date')

# Fixture: Sample DataFrame with non-datetime index
@pytest.fixture
def non_datetime_dataframe() -> pd.DataFrame:
    """
    Fixture for creating a sample DataFrame with non-datetime index.
    """
    data = {'value': [1, 2, 3]}
    index = [1, 2, 3]
    return pd.DataFrame(data, index=index).rename_axis('Date')


def test_sample_date_dataframe(sample_date_dataframe):
    TimeSeriesAccessor(sample_date_dataframe)  # Should not raise any exceptions

def test_non_datetime_dataframe(non_datetime_dataframe):
    with pytest.raises(ValueError, match="Index must be named 'Date' and in the pandas datetime format."):
        TimeSeriesAccessor(non_datetime_dataframe)

def test_noname_dataframe(sample_noname_dataframe):
    with pytest.raises(ValueError, match="Index must be named 'Date' and in the pandas datetime format."):
        TimeSeriesAccessor(sample_noname_dataframe)

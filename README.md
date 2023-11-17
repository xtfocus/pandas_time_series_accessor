# TimeSeriesAccessor for Pandas

The `TimeSeriesAccessor` is a custom accessor for pandas that provides additional functionality for working with time series data.

## Installation

Install the required packages:

```bash
pip install pandas matplotlib missingno
```

## Usage

```python3
import pandas as pd
from TimeSeries import TimeSeriesAccessor  

# Load your time series data into a DataFrame
data = {'Date': ['2022-01-01', '2022-01-02', '2022-01-05', '2022-01-06'],
        'Value': [10, 15, 20, 25]}
df = pd.DataFrame(data)
df['Date'] = pd.to_datetime(df['Date'])
df.set_index('Date', inplace=True)

# EDA: missing rows and biggest gaps, business days or any days
df.ts.perform_eda()

# Fill forward missing values for business days
filled_df = ts_accessor.fill_forward(business=True)

# Show biggest 2 business days gaps
df.ts.find_biggest_gaps(business=True, k=2)
```

## Functions

### `find_biggest_gaps(business: bool = False, k: int = 5) -> pd.DataFrame`

Find the biggest gap (consecutive days missing) in the DataFrame.

- `business`: If True, only report gaps caused by consecutive business working days.
- `k`: Number of top gaps to report.

### `perform_eda() -> None`

Perform exploratory data analysis on each DataFrame.

### `fill_forward(business: bool = True) -> pd.DataFrame`

Fill forward missing values in the DataFrame.

- `business`: If False, fill forward for all days. If True, fill forward only for business days.

### `remove_weekend_days() -> pd.DataFrame`

Remove weekend days (Saturday and Sunday) from the DataFrame.

### `report_missing_days(business: bool = True) -> List[pd.Timestamp]`

Report missing days in a DataFrame. Include value counts of weekdays for the missing days.

- `business`: If True, reports only for missing business days.

# Azure Functions: Find Missing Data

This project contains an Azure Function that is designed to find missing data in a given dataset.

## Project Structure

This project follows the repository pattern for organizing code. The main components of the project are:

- `Repositories`: This directory contains the code for data access and manipulation.
- `Services`: This directory contains the business logic of the application.
- `Common`: This directory contains shared code that is used across multiple parts of the application. This can include utility functions, constants, and other reusable components.

## Key Methods

### `calculate_missing_timestamps`

This method is used to find missing timestamps in a given dataset. It takes the following parameters:

- `rows`: The data to check for missing timestamps.
- `frequency`: The frequency of the expected data ('5min', '30min', 'H').
- `start`: The start of the time range to check.
- `end`: The end of the time range to check.
- `table_name`: The name of the table to check.
- `table_column_map`: A map of table names to their columns.

This method returns a list of datetime strings for the missing data.

### Behavior

The function first checks if `rows` is None or empty. If it is, the function generates a complete list of datetime strings between `start` and `end` with the specified `frequency` and returns it.

If `rows` is not None or empty, the function generates a complete list of datetime strings between `start` and `end` with the specified `frequency`. It then subtracts the timestamps in `rows` from the complete list and returns the result.

For the '30min' frequency, the function rounds down the timestamps in `rows` to the nearest half hour before subtracting them from the complete list.


## Example Input and Output

### Input


```json
{
    "table_times": {
        "PREDISPATCHREGIONSUM": {
            "start_timestamp": "2025-12-20 00:00:00.000",
            "end_timestamp": "2026-01-01 15:00:00.000"
        },
        "PRICE": {
            "start_timestamp": "2022-12-01 00:00:00.000",
            "end_timestamp": "2022-12-01 15:00:00.000"
        },
        "REGIONSUM":
        {
            "start_timestamp": "2022-12-01 00:00:00.000",
            "end_timestamp": "2022-12-01 15:00:00.000"
        }
    }
}
 
```

### Output 
```json

{
    "table_times": [
        {
            "table_name": "PREDISPATCHREGIONSUM",
            "missing_timestamps": [
                "2025-12-20T00:00:00",
                "2025-12-20T00:30:00",
                "2025-12-20T01:00:00",
                "2025-12-20T01:30:00",
                "2025-12-20T02:00:00",
                "2025-12-20T02:30:00",
                .....

  
            ],
            "start_timestamp": "2025-12-20 00:00:00.000",
            "end_timestamp": "2026-01-01 15:00:00.000",
            "length": 607,
            "frequency": "30min"
        },
        {
            "table_name": "PRICE",
            "missing_timestamps": [],
            "start_timestamp": "2022-12-01 00:00:00.000",
            "end_timestamp": "2022-12-01 15:00:00.000",
            "length": 0,
            "frequency": "5min"
        },
        {
            "table_name": "REGIONSUM",
            "missing_timestamps": [],
            "start_timestamp": "2022-12-01 00:00:00.000",
            "end_timestamp": "2022-12-01 15:00:00.000",
            "length": 0,
            "frequency": "5min"
        }
    ]
}
```




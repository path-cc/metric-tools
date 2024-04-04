# campuses-with-active-researchers.py

Prints list of campuses with active researchers in the given date range, along with whether they're CC* or not.

See [SOFTWARE-4396](https://opensciencegrid.atlassian.net/browse/SOFTWARE-4396) for the definition of 'active researcher'.


## Requirements/Installation

Requires Python 3 with the `opensearch-py` module.

Installation (assuming Python 3 is already installed):
```
git clone https://github.com/path-cc/metric-tools
cd metric-tools/campuses-with-activate-researchers
python3 -m venv .
. ./bin/activate
pip install -r requirements.txt
```

## Usage

```
./campuses-with-active-researchers.py [--csv] <START DATE> <END DATE>
```
`START DATE` and `END DATE` should be in `YEAR-MONTH-DAY` format, e.g. `2020-01-01`.
The output includes the data from 00:00:00 on the start date to 23:59:59 on the end date.

If you pass `--csv`, the script will print the results in a CSV format with headers.
Otherwise, the script will print a human-readable table.

Examples:

* Print the list for the calendar year 2020 in a human-readable table:
```
./campuses-with-active-researchers.py 2020-01-01 2020-12-31
```

* Save the list for the first three months of 2020 in CSV format:
```
./campuses-with-active-researchers.py --csv 2020-01-01 2020-03-31 > campuses-jan-to-mar.csv
```

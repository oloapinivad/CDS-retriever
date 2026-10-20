# CDS-retriever
**Tool for parallel retrieval of ECMWF ERA5 data from the Climate Data Store (CDS)**

## Overview
CDS-retriever automates the parallel download and postprocessing of ERA5 and ERA5-Land data from the Climate Data Store (CDS).

It is built on top of:

- `cdsapi` (>= 0.7.7, the client for the new CDS infrastructure) for data retrieval
- `CDO` and its Python bindings for postprocessing

## Main features

- Parallelization is performed on a **yearly basis**.
- Variables sharing the same properties are **downloaded with a single request** per year when it fits
  the CDS size limit (`group_variables: auto`), then split per variable: the CDS queues every request,
  so e.g. 30 monthly-mean variables take one request per year instead of 30.
- Requests are **validated against the CDS catalogue** before downloading (variable names, years, levels...),
  so that mistakes are reported immediately instead of after hours in the queue.
- Request size is checked with the CDS cost estimate: with `download_request: auto` yearly requests
  are used when allowed, otherwise the year is split in monthly requests
  (e.g. hourly data on 37 pressure levels exceed the CDS limit for a single year).
  Monthly requests are run in parallel as independent (year, month) jobs and then concatenated per year.
- Failed requests are retried; files are written atomically and checked for completeness, so
  interrupted runs can be safely restarted and already downloaded files are skipped.
- Errors in the parallel workers are reported and the program exits with a non-zero status.
- Post-processing converts the downloaded GRIB files to compressed NetCDF4 format.
- For monthly data, the script merges the yearly files and optionally aligns the time axis.
- For sub-daily data, daily mean files are generated.

## Set-up
It is recommended to use conda to isolate dependencies (CDO and ecCodes are not pip-installable):

```
conda env create -f environment.yml
```

This installs the package in editable mode and provides the `cds-retriever` command.
In an existing environment with CDO available you can also use `pip install -e .`.

You need a CDS account: accept the licence of each dataset you want to download on its web page
and set up your `~/.cdsapirc` with your Personal Access Token
(see the [CDS API how-to](https://cds.climate.copernicus.eu/how-to-api)):

```
url: https://cds.climate.copernicus.eu/api
key: <PERSONAL-ACCESS-TOKEN>
```

## Usage
1. Activate the environment

```
conda activate CDS-retriever
```

2. Set the options in the config file (see below for a list of the currently implemented options).
You can set up your config file depending on the data you need starting from the provided example `config.example.yaml`

3. Check your configuration without downloading anything: requests are validated and their cost is shown

```
cds-retriever -c <path-to-config-file>/config.yaml --dry-run
```

4. Run the retrieval and postprocessing:

```
cds-retriever -c <path-to-config-file>/config.yaml
```

The old `./ERA5_retrieve_postproc.py -c config.yaml` entry point is still available and equivalent.

Optionally, you can override configuration options using command-line arguments:

| **Argument**       | **Description**                     |
|--------------------|-------------------------------------|
| `-c`, `--config`   | Path to the YAML configuration file |
| `-n`, `--nprocs`   | Number of parallel processes        |
| `-u`, `--update`   | Update existing dataset             |
| `-v`, `--variable` | Select a specific variable          |
| `-l`, `--levelout` | Select a specific level             |
| `--outputdir`      | Specify output directory            |
| `--tmpdir`         | Specify temporary directory         |
| `--dry-run`        | Validate requests and show their cost, do not download |
| `--loglevel`       | Logging level (`DEBUG`, `INFO`, `WARNING`) |

Example:

```
cds-retriever -c <path-to-config-file>/config.yaml -n 2 -l 500hPa
```
**Note:** If the same option is specified both in the configuration file and via command-line arguments,
the **command-line argument takes precedence.**

## Configuration Parameters

| **Parameter**        | **Type**            | **Description**                                                                    |
|----------------------|---------------------|------------------------------------------------------------------------------------|
| `tmpdir`, `storedir` | `str`               | Paths for temporary download and final storage directories (required).             |
| `dataset`            | `str`               | Dataset to retrieve. Options: **`'ERA5'`** (default), **`'ERA5-Land'`**.           |
| `product_type`       | `str`               | **`'reanalysis'`** (default), or ERA5 ensemble **`'ensemble_members'`**, **`'ensemble_mean'`**, **`'ensemble_spread'`** (3-hourly). |
| `varlist`            | `list[str]`         | List of variables to download (e.g., `['total_precipitation', '2m_temperature']`). All variables must share the same properties. |
| `year.begin`         | `int`               | First year of the time range to retrieve.                                          |
| `year.end`           | `int`               | Last year of the time range to retrieve.                                           |
| `year.update`        | `bool`              | If **`True`**, the script extends an existing dataset up to the last complete year. Overrides `year.begin` and `year.end`.  |
| `freq`               | `str`               | Data frequency. Options: **`'instant'`**, **`'1hr'`**, **`'3hrs'`**, **`'6hrs'`**, **`'mon'`** (`'mon'` = monthly means, default). |
| `levelout`           | `str`/`list[str]`   | Vertical level selection. Options: **`'sfc'`** (default), **`'plev37'`**, **`'plev19'`**, **`'plev8'`**, a single level (e.g. **`'500hPa'`**) or a list of levels (e.g. **`['500hPa', '850hPa']`**). |
| `grid`               | `str`               | Output grid resolution. Options: **`'full'`** (default, native grid), **`'0.1x0.1'`**, **`'0.25x0.25'`**, **`'0.5x0.5'`**, **`'1x1'`**, **`'2.5x2.5'`**. |
| `area`               | `str`/`list[float]` | Geographic area for download. Either **`'global'`** or a list of coordinates `[North, West, South, East]`, e.g., `[65, -15, 25, 45]`. |
| `nprocs`             | `int`               | Number of parallel processes (default 4), i.e. the maximum number of simultaneous CDS requests. Each job is a year, or a single month with monthly requests. Keep it moderate, since the CDS limits the queued requests per user. |
| `download_request`   | `str`               | Chunking mode for retrieval. Options: **`'auto'`** (default), **`'yearly'`**, **`'monthly'`**. |
| `retries`            | `int`               | Attempts for each request before giving up (default 3).                            |
| `check_request`      | `bool`              | Validate requests against the CDS catalogue before downloading (default **`True`**). |
| `group_variables`    | `str`/`bool`        | **`'auto'`** (default): one request per year for all the variables (with the same years to retrieve) if within the CDS size limit, split afterwards with ecCodes `grib_copy`. **`False`**: one request per variable. |
| `do_retrieve`        | `bool`              | If **`True`**, performs download from the Climate Data Store.                      |
| `do_postproc`        | `bool`              | If **`True`**, runs postprocessing with **CDO**.                                   |
| `do_align`           | `bool`              | If **`True`**, aligns monthly time axes (to 00:00) for **Xarray** compatibility.   |
| `merge`              | `bool`              | If **`True`** (default), monthly data are merged in a single multi-year file; otherwise yearly files are kept. |

## Use as a library

`retrieve` takes the same configuration as a dictionary (or the path of a YAML file), with the
defaults filled in, and returns the requests made and the netCDF files produced for each variable:

```python
from cds_retriever import retrieve, last_available, RetrievalError

print(last_available('ERA5', freq='mon', levelout='sfc'))   # e.g. 2026-09-01, from the CDS catalogue

result = retrieve({'tmpdir': '/scratch/era5', 'storedir': '/data/era5',
                   'varlist': ['2m_temperature', 'total_precipitation'],
                   'merge': False}, years=[2024, 2025])
result.files      # {'2m_temperature': [Path('.../ERA5_2m_temperature_mon_full_sfc_2024.nc'), ...], ...}
result.requests   # one Request per CDS request group, with its cost when known
```

- `variables=` and `years=` override `varlist` and `year`; `dry_run=True` only validates and
  prices the requests.
- Failures raise `RetrievalError`, with `failures` (job label -> exception) and `result` (what was
  produced anyway). Invalid configurations raise `ValueError`.
- The library never configures logging: messages go to the `cds_retriever` logger.
- In update mode the last year retrieved is the last complete year published on the CDS.

## Output

Downloaded GRIB files are kept in `<tmpdir>/<variable>/`, while netCDF files are stored in
`<storedir>/<variable>/<freq>/` (and `<storedir>/<variable>/day/` for daily means), named as
`<dataset>_<variable>_<freq>_<grid>_<levelout>_<years>[_<area>].nc`.

The naming scheme is the same as in previous versions (checked by `tests/test_naming_compat.py`),
so existing archives can be extended with `year.update` / `--update`.
Daily means are recomputed from all the yearly sub-daily files, and the previous daily file
of the same dataset is removed (e.g. `..._day_..._1979-2019.nc` replaced by `..._1979-2025.nc`),
unless it covers years that are no longer available as sub-daily files.
Exception: with an `area` selection, older versions wrote merged monthly and daily files with
wrong years taken from the area string (e.g. `..._40-40_70_-20_30_40.nc`). The update reports
these files: rename them with the actual years (e.g. `..._1940-2022_70_-20_30_40.nc`) and run again.

## Development

```
pip install -e ".[test]"
pytest                   # all tests
pytest -m "not network"  # skip tests querying the public CDS catalogue
```

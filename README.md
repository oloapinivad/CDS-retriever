# CDS-retriever
**Tool for parallel retrieval of ECMWF ERA5 data from the Climate Data Store (CDS)**

## Overview
CDS-retriever automates the parallel download and postprocessing of ERA5 data from the Climate Data Store (CDS).

It is built on top of:

- `cdsapi` for data retrieval
- `CDO` and its Python bindings for postprocessing

Additional Python packages such as `netCDF4` and `xarray` are recommended (though not strictly required).

## Main features

- Parallelization is performed on a **yearly basis**.
- Post-processing converts the downloaded GRIB files to NetCDF format.
- For monthly data, the script can merge files and optionally align the time axis.
- For hourly data, daily mean files are optionally generated.

## Set-up
Install the requested dependencies.
It is recommended to use conda to isolate dependencies:

```
conda env create -f environnment.yaml -n CDS-retriever
```

## Usage
1. Be sure that all required dependecies are loaded, or equivalently , load the above created environment

```
conda activate CDS-retriever
```

2. Set the options in the config file (see below for a list of the currently implemented options).
You can set up your config file dependign on the data you need starting from the provided template `config.tmpl`

3. Run the main script with a configuration file:

```
./ERA5_retrieve_postproc.py -c <path-to-config-file>/config.yaml
```

Optionally, you can override configuration options using command-line arguments:

| **Argument**       | **Description**                     |
|--------------------|-------------------------------------|
| `-c`, `--config`   | Path to the YAML configuration file |
| `-n`, `--nprocs`   | Number of parallel processes        |
| `-u`, `--update`	 | Update existing dataset             |
| `-v`, `--variable` | Select a specific variable          |
| `-l`, `--levelout` | Select a specific level             |
| `--outputdir`	     | Specify output directory            |
| `--tmpdir`	     | Specify temporary directory         |

Example:

```
./ERA5_retrieve_postproc.py -c <path-to-config-file>/config.yaml -n 2 -l 500hPa
```
**Note:** If the same option is specified both in the configuration file and via command-line arguments, 
the **command-line argument takes precedence.**

## Configuration Parameters

| **Parameter**        | **Type**            | **Description**                                                                    |
|----------------------|---------------------|------------------------------------------------------------------------------------|
| `tmpdir`, `storedir` | `str`               | Paths for temporary download and final storage directories.                        |
| `dataset`            | `str`               | Dataset to retrieve. Options: **`'ERA5'`**, **`'ERA5-Land'`**.                     |
| `varlist`            | `list[str]`         | List of variables to download (e.g., `['total_precipitation', '2m_temperature']`). All variables must share the same properties. |
| `year.begin`         | `int`               | First year of the time range to retrieve.                                          |
| `year.end`           | `int`               | Last year of the time range to retrieve.                                           |
| `year.update`        | `bool`              | If **`True`**, the script extends an existing dataset instead of redownloading it. Overrides `year.begin` and `year.end`.  |
| `freq`               | `str`               | Data frequency. Options: **`'instant'`**, **`'1hr'`**, **`'6hrs'`**, **`'mon'`** (`'mon'` = monthly means). | 
| `levelout`           | `str`               | Vertical level selection. Options: **`'sfc'`**, **`'plev37'`**, **`'plev19'`**, **`'plev8'`**, a single level (e.g. **`'500hPa'`**). |
| `grid`               | `str`               | Output grid resolution. Options: **`'full'`**, **`'0.1x0.1'`**, **`'0.25x0.25'`**, **`'2.5x2.5'`**. (`'full'` = native ERA5 grid). |
| `area`               | `str`/`list[float]` | Geographic area for download. Either **`'global'`** or a list of coordinates `[North, West, South, East]`, e.g., `[65, -15, 25, 45]`. |
| `nprocs`             | `int`               | Number of parallel processes.                                                      |
| `download_request`   | `str`               | Chunking mode for retrieval. Options: **`'yearly'`**, **`'monthly'`**.             |
| `do_retrieve`        | `bool`              | If **`True`**, performs download from the Climate Data Store.                      |
| `do_postproc`        | `bool`              | If **`True`**, runs postprocessing with **CDO**.                                   |
| `do_align`           | `bool`              | If **`True`**, aligns monthly time axes for **Xarray** compatibility.              |


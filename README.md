# CDS-retriever
Too for parallel retrieve of ECMWF ERA5 (and ERA5-land) from the Climate Data Store.

It is built on cdsapi and uses CDO and its python bindings for the postprocessing. Netcdf4 and xarray are also recommended altough not stricly required. Parallelization is done on yearly basis. Postprocessing provides daily and monthly files using CDO. 

You can configure the `ERA5_retrieve_postproc.py` command line and run it using python3.
The config.yaml attached is an example of the things you can provide

It is recommended to run it under conda to do not mess with dependencies

```
mamba env create -f environment.yaml
mamba activate CDS
./ERA5_retrieve_postproc.py -n 3
```

You can configure:
- Temporary and storage directories: `tmpdir` and `storedir`
- Variable to be downloaded: `var`
- First and last year to be extracted: `year1` and `year2`
- Number of processor for parallel download: `nprocs` (it uses the `multiprocessing` python package, use the -n flag)
- Level you want to download: `levelout`(it supports surface and a few predefined pressure levels)
- Grid on which you want to download: `grid`
- Area on which download: `area` (it could be global or sub-selected according to CDS vocabulary)
- `--update` flag to just update the archive to the last year available
- `--config` to use multiple config files


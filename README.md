# CDS-retriever
Too for parallel retrieve of ECMWF ERA5 from the Climate Data Store.

It is built on cdsapi and uses CDO and its python bindings for the postprocessing. Netcdf4 and xarray are also recommended altough not stricly required. Parallelization is done on yearly basis. Postprocessing provides daily and monthly files using CDO. 

You can configure the `ERA5_retrieve_postproc.py` script (no command line interface but manual configuration required) and run it using python3.

It is recommended to run it under conda to do not mess with dependencies

```
conda create -n CDS python=3.9
conda install -n CDS -c conda-forge cdo python-cdo cdsapi netcdf4 xarray
conda activate CDS
./ERA5_retrieve_postproc.py
```

You can configure:
- Temporary and storage directories: `tmpdir` and `storedir`
- Variable to be downloaded: `var`
- First and last year to be extracted: `year1` and `year2`
- Number of processor for parallel download: `nprocs` (it uses the `multiprocessing` python package)
- Level you want to download: `levelout`(it supports surface and a few predefined pressure levels)
- Grid on which you want to download: `grid`
- Area on which download: `area` (it could be global or sub-selected according to CDS vocabulary)


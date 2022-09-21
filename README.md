# CDS-retriever
Too for parallel retrieve of ECMWF ERA5 from the Climate Data Store.

It is built on cdsapi and uses CDO and its python bindings for the postprocessing. Netcdf4 and xarray are also recommended altough not stricly required. 

You can configure the `ERA5_retrieve_postproc.py` script (no command line interface but manual configuration required) and run it using python3 

It is recommended to run it under conda to do not mess with dependencies

```
conda create -n CDS python=3.9
conda install -n CDS -c conda-forge cdo python-cdo cdsapi netcdf4 xarray
conda activate CDS
./ERA5_retrieve_postproc.py
```



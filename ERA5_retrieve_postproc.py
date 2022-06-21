#!/usr/bin/env python

# basic python3 scripts to retrieve ERA5 data from the CDS, to replace old Bash scripts 
# Parallel retrieval is done as a function of the years (set nprocs)
# A single variable at time can be retrieved
# Data are downloaded in grib and then archived in netcdf4 zip using CDO bindings
# Monthly means as well as hourly data can be downloaded
# Multiple grids are supported
# Both surface variables and pressure levels are supported. 
# @ Author: Paolo Davini, CNR-ISAC, Jun 2022

import os
from pathlib import Path
from cdo import Cdo
from multiprocessing import Process
from CDS_retriever import year_retrieve
 
 # where this is downloaded
tmpdir = '/work/scratch/users/paolo/era5'

# where data is stored
storedir = 'work/datasets/obs/ERA5'

# the variable you want to retrieve
var = 'geopotential'
year1 = 1990
year2 = 1999

# parallel process
nprocs = 10

#### - Frequency ---  ####
freq='mon'
#freq='6hrs'
#freq='1hr'

##### - Vertical levels ---- ####
# for surface vars
#levelout = 'sfc' 

# for plev variables
#levelout='plev19'
#levelout='plev8'

# for single pressure level vars
levelout='500hPa'

##### - Grid selection ---- ####
#grid = '0.25x0.25'
grid = '2.5x2.5'

#### - control for the structure --- ###
do_retrieve = True # retrieve data from CDS
do_postproc = False # postproc data with CDO


if do_retrieve: 

    # define the out dir and file 
    savedir =  os.path.join(outdir, var)
    Path(outdir).mkdir(parents=True, exist_ok=True)

    years = [str(i) for i in range(year1,year2+1)]
    #years = ['1990', '1991', '1992']

    # loop on vars and years
    #for year in years : 
    #    year_retrieve(var, freq, year, grid, levelout, outdir)

    # loop on the years create the parallel process
    processes = []
    yearlist = [years[i:i + nprocs] for i in range(0, len(years), nprocs)]
    for lyears in yearlist:
        print(lyears)
        for year in lyears : 
            print(year)
            p = Process(target=year_retrieve, args=(var, freq, year, grid, levelout, tmpdir))
            p.start()
            processes.append(p)

        # wait for all the processes
        for process in processes:
            process.join()
   


if do_postproc :

    destdir = os.path.join(storedir, var, freq)
    Path(destdir).mkdir(parents=True, exist_ok=True)

    for year in years :
        filename = 'ERA5_' + var + '_' + freq + '_' + grid + '_' + levelout + '_' + year
        infile = os.path.join(outdir, destdir + '.grib')
        outfile = os.path.join(destdir, filename + '.nc')
        Cdo.copy(infile, outfile, options = '-f nc4 -z zip')

  


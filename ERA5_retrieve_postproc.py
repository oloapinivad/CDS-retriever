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
import glob
from CDS_retriever import year_retrieve, year_convert, create_filename, first_last_year
cdo=Cdo()

 
######## -----   USER CONFIGURATION ------- ########

 # where this is downloaded
tmpdir = '/work/scratch/users/paolo/era5'

# where data is stored
#storedir = '/work/datasets/obs/ERA5'
storedir = '/work/scratch/users/paolo/ERA5'

# the variable you want to retrieve 
var = 'geopotential'

# the years you need to retrieve
year1 = 1990
year2 = 1991

# parallel process
nprocs = 2

#### - Frequency ---  ####
# three different options, monthly get monthly means. 
#freq='mon'
freq='6hrs'
#freq='1hr'

##### - Vertical levels ---- ####
# multiple options for surface levels and for pressure levels

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
do_postproc = True # postproc data with CDO

######## ----- END OF USER CONFIGURATION ------- ########

# create list of years
years = [str(i) for i in range(year1,year2+1)]
#years = ['1990', '1991', '1992']

# define the out dir and file 
savedir =  Path(tmpdir, var)
Path(savedir).mkdir(parents=True, exist_ok=True)

# retrieve block
if do_retrieve: 

    # loop on the years create the parallel process
    processes = []
    yearlist = [years[i:i + nprocs] for i in range(0, len(years), nprocs)]
    for lyears in yearlist:
        for year in lyears : 
            print(year)
            p = Process(target=year_retrieve, args=(var, freq, year, grid, levelout, savedir))
            p.start()
            processes.append(p)

        # wait for all the processes to end
        for process in processes:
            process.join()

#  
if do_postproc :

    destdir = Path(storedir, var, freq)
    Path(destdir).mkdir(parents=True, exist_ok=True)

    # loop on the years create the parallel process for a fast conversion
    processes = []
    yearlist = [years[i:i + nprocs] for i in range(0, len(years), nprocs)]
    for lyears in yearlist:
        for year in lyears : 
            print(year)
            filename = create_filename(var, freq, grid, levelout, year)
            infile = Path(savedir, filename + '.grib')
            outfile = Path(destdir, filename + '.nc')
            p = Process(target=year_convert, args=(infile, outfile))
            #p = Process(target=cdo.copy, args=(infile, outfile, '-f nc4 -z zip'))
            p.start()
            processes.append(p)

        # wait for all the processes to end
        for process in processes:
            process.join()

    # extra processing for monthly data
    if freq == "mon" : 
        filepattern = Path(destdir, create_filename(var, freq, grid, levelout, '????') + '.nc')
        first_year, last_year = first_last_year(filepattern)
        mergefile = Path(destdir, create_filename(var, freq, grid, levelout, first_year + '-' + last_year) + '.nc')
        if os.path.exists(mergefile):
            os.remove(mergefile)
        cdo.cat(input = filepattern, output = mergefile)
        for f in glob.glob(filepattern): 
            os.remove(f)

    # extra processing for daily data
    else : 
        daydir, mondir = [Path(storedir, var, x) for x in ['day', 'mon']]   
        Path(daydir).mkdir(parents=True, exist_ok=True)
        Path(mondir).mkdir(parents=True, exist_ok=True)

        filepattern = Path(destdir, create_filename(var, freq, grid, levelout, '????') + '.nc')
        first_year, last_year = first_last_year(filepattern)
        
        dayfile = Path(daydir, create_filename(var, 'day', grid, levelout, first_year + '-' + last_year) + '.nc')
        monfile = Path(mondir, create_filename(var, 'mon', grid, levelout, first_year + '-' + last_year) + '.nc')

        if os.path.exists(dayfile):
            os.remove(dayfile)
        cdo.daymean(input = '-cat ' + filepattern, outfile = dayfile, options = '-f nc4 -z zip')
        cdo.monmean(input = dayfile, output = monfile, options = '-f nc4 -z zip')
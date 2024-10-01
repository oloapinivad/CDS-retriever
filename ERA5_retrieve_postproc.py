#!/usr/bin/env python3

# basic python3 scripts to retrieve ERA5 data from the CDS, to replace old Bash scripts 
# Parallel retrieval is done as a function of the years (set nprocs)
# A single variable at time can be retrieved
# Data are downloaded in grib and then archived in netcdf4 zip using CDO bindings
# Monthly means as well as hourly data can be downloaded
# Multiple grids are supported
# Both surface variables and pressure levels are supported. 
# Support for area selection has been included
# @ Author: Paolo Davini, CNR-ISAC, Jun 2022

import os
import shutil
import glob
import logging
from pathlib import Path
from cdo import Cdo
from multiprocessing import Process
from CDS_retriever import year_retrieve, year_convert, create_filename, first_last_year, which_new_years_download
cdo=Cdo()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 
######## -----   USER CONFIGURATION ------- ########

 # where data is downloaded
#tmpdir = '/scratch/b/b382076/era5'
tmpdir = '/work/scratch/users/paolo/era5'

# where data is stored
#storedir = '/work/bb1153/b382076/ERA5'
storedir = '/work/datasets/obs/ERA5'

# which ERA dataset you want to download: only ERA5 and ERA5-Land available
dataset = 'ERA5'
#dataset = 'ERA5-Land'

# the list of variables you want to retrieve  (CDS format)
# please note they must share the same properties!
#varlist = ['top_net_solar_radiation', 'top_net_thermal_radiation', 'total_precipitation']
#varlist = 'evaporation'
#varlist = '2m_temperature'
varlist = ['2m_dewpoint_temperature', 'surface_solar_radiation_downwards', '10m_u_component_of_wind', '10m_v_component_of_wind']
#varlist = ['10m_u_component_of_wind', '10m_v_component_of_wind']
varlist = ['geopotential']


# the years you need to retrieve
# so far anythin before 1959 is calling the preliminary dataset
year1 = 1940
year2 = 2023

# option to extend current dataset
# this will superseed the year1/year2 values
update = False

# parallel processes
nprocs = 12

#### - Frequency ---  ####
# three different options, monthly get monthly means. 
#freq = 'mon'
freq = '6hrs'
#freq = '1hr'
#freq = 'instant' #beware


##### - Vertical levels ---- ####
# multiple options for surface levels and for pressure levels

# for surface vars
#levelout = 'sfc' 

# for plev variables
#levelout='plev37'
#levelout='plev19'
#levelout='plev8'

# for single pressure level vars
levelout = '500hPa'

##### - Grid selection ---- ####
# any format that can be interpreted by CDS
# full means that no choiche is made, i.e. the original grid is provided
#grid = 'full'
grid = '0.25x0.25'
#grid = '0.1x0.1'
#grid = '2.5x2.5'


##### - Region ---- ####
# 'global' or any format that can be interpeted by CDS
# the order should be North, West, South, East
area = 'global'
#area =  [49, 4, 43, 19]

##### - Download request ---- ####
# do you want to download yearly chunks or monthly chunks?
download_request='yearly'

#### - control for the structure --- ###
do_retrieve = True # retrieve data from CDS
do_postproc = True # postproc data with CDO
do_align = False #set equal time axis to monthly data to work with Xarray

######## ----- END OF USER CONFIGURATION ------- ########

# safecheck
if isinstance(varlist, str):
    varlist = [varlist]

for var in varlist:

    if update:
        logging.info("Update flag is true, detection of years...")
        year1, year2 = which_new_years_download(storedir, dataset, var, freq, grid, levelout, area)
        logging.info(year1, year2)
        if year1 > year2:
            logging.info('Everything you want has been already downloaded, disabling retrieve...')
            do_retrieve=False
            if (freq == 'mon'):
                logging.info('Everything you want has been already postprocessed, disabling postproc...')
                do_postproc=False


    # create list of years
    years = [str(i) for i in range(year1,year2+1)]

    # define the out dir and file
    savedir =  Path(tmpdir, var)
    Path(savedir).mkdir(parents=True, exist_ok=True)

    # retrieve block
    if do_retrieve:

        # loop on the years create the parallel process
        processes = []
        yearlist = [years[i:i + nprocs] for i in range(0, len(years), nprocs)]
        for lyears in yearlist:
            for year in lyears: 
                logging.info(year)
                p = Process(target=year_retrieve, args=(dataset, var, freq, year, grid, levelout, 
                                                        area, savedir, download_request))
                p.start()
                processes.append(p)

            # wait for all the processes to end
            for process in processes:
                process.join()

    # postproc
    if do_postproc :

        cdo.debug=True

        logging.info('Running postproc...')
        destdir = Path(storedir, var, freq)
        Path(destdir).mkdir(parents=True, exist_ok=True)

        # loop on the years create the parallel process for a fast conversion
        processes = []
        yearlist = [years[i:i + nprocs] for i in range(0, len(years), nprocs)]
        for lyears in yearlist:
            for year in lyears:
                filename = create_filename(dataset, var, freq, grid, levelout, area, year)
                infile = Path(savedir, filename + '.grib')
                outfile = Path(destdir, filename + '.nc')
                logging.info('Conversion of %s from file %s to file %s', year, infile, outfile)
                p = Process(target=year_convert, args=(infile, outfile))
                #p = Process(target=cdo.copy, args=(infile, outfile, '-f nc4 -z zip'))
                p.start()
                processes.append(p)

            # wait for all the processes to end
            for process in processes:
                process.join()

            logging.info('Conversion complete!')

        # extra processing for monthly data
        if freq == "mon" :
            logging.info('Extra processing for monthly...')

            
            filepattern = str(Path(destdir, create_filename(dataset, var, freq, grid, levelout, area, '????') + '.nc'))
            first_year, last_year = first_last_year(filepattern, area=area)

            if update:
                # check if big file exists
                bigfile = str(Path(destdir, create_filename(dataset, var, freq, grid, levelout, area, '????', '????') + '.nc'))
                filebase = glob.glob(bigfile)
                first_year, _ = first_last_year(bigfile, area=area)
                filepattern = filebase + glob.glob(filepattern)

            mergefile = str(Path(destdir, create_filename(dataset, var, freq, grid, levelout, area, first_year + '-' + last_year) + '.nc'))
            logging.info('Removing existing file %s...', mergefile)
            if os.path.exists(mergefile):
                os.remove(mergefile)
            logging.info('Merging together into %s...', mergefile)
            cdo.cat(input=filepattern, output=mergefile, options='-f nc4 -z zip')
            if isinstance(filepattern, str):
                loop = glob.glob(filepattern)
                for f in loop:
                    os.remove(f)

            # HACK: set a common time axis for monthly data (roll back cumulated by 6hours). useful for catalog xarray loading 
            if do_align:
                logging.info('Aligningment required...')
                first_time=cdo.showtime(input=f'-seltimestep,1 {mergefile}')[0]
                if first_time != '00:00:00':
                    tempfile = str(Path(tmpdir, 'temp_align.nc'))
                    shutil.move(mergefile, tempfile)
                    cdo.shifttime('-6hours', input = tempfile, output = mergefile, options = '-f nc4 -z zip')
                    os.remove(tempfile)


        # extra processing for daily data
        else: 
            logging.info('Extra processing for daily and 6hrs...')
            daydir, mondir = [Path(storedir, var, x) for x in ['day', 'mon']]   
            Path(daydir).mkdir(parents=True, exist_ok=True)
            Path(mondir).mkdir(parents=True, exist_ok=True)

            filepattern = Path(destdir, create_filename(dataset, var, freq, grid, levelout, area, '????') + '.nc')
            first_year, last_year = first_last_year(filepattern, area=area)
            
            dayfile = str(Path(daydir, create_filename(dataset, var, 'day', grid, levelout, area, first_year + '-' + last_year) + '.nc'))
            monfile = str(Path(mondir, create_filename(dataset, var, 'mon', grid, levelout, area, first_year + '-' + last_year) + '.nc'))

            if os.path.exists(dayfile):
                os.remove(dayfile)

            cdo.daymean(input = '-cat ' + str(filepattern), output = dayfile, options = '-f nc4 -z zip')
            cdo.monmean(input = dayfile, output = monfile, options = '-f nc4 -z zip')

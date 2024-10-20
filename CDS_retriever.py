import os
import sys
import glob
from pathlib import Path
from pprint import pprint
import datetime
import cdsapi
from cdo import Cdo, CDOException
from urllib3.exceptions import IncompleteRead

cdo = Cdo()

class MaxAttemptsError(Exception):
    pass

# check with cdo is the file is complete (approximately correct)
def is_file_complete(filename, minimum_steps):
    """
    Is a file that we want to download complete?
    
    Returns:
      a boolean, True if the file is ok, False if the file need to be downloaded
    
    """

    # set it false by default
    filename = str(filename)

    # if file exists
    if os.path.exists(filename):

        try:
            # cdo ntime return a list with the length of the timesteps, select the first one
            nt = int(cdo.ntime(input=filename, options = '-s')[0])
            print(f'The file has {nt} timesteps...')

            # if the number of steps is not enough...
            if nt < minimum_steps:
                print(f'The file {filename} looks incomplete with nsteps {nt} < {minimum_steps} minimum steps')
                return False
            else:
                print(filename + ' is complete! Going to next one...')
                return True

        except (KeyError, CDOException):
            print (filename + ' is corrupted')
            return False

    # if file does not exist
    print (filename + ' is missing')
    return False

    
# big function for retrieval
def year_retrieve(dataset, var, freq, year, grid, levelout, area, outdir, request = 'yearly',max_attemps=5): 

    # year for preliminary era5 reanalysis - now deprecated
    #year_preliminary = 1900

    # configuration part (level)
    level, level_kind = define_level(levelout)
    if dataset == 'ERA5':
        kind = 'reanalysis-era5-' + level_kind
    elif dataset == 'ERA5-Land':
        kind = 'reanalysis-era5-land'
    else: 
        sys.exit('Unknown dataset!')

    # extract time information
    product_type, day, time, time_kind, minimum_steps = define_time(freq)
    kind = kind + time_kind

    # set up the months loop
    if request=='yearly':
        months = [[str(i).zfill(2) for i in range(1,12+1)]]
    elif request=='monthly':
        months = [str(i).zfill(2) for i in range(1,12+1)]
    else:
        sys.exit('Wrong download request!')

    # check if yearly file is complete
    basicname = create_filename(dataset, var, freq, grid, levelout, area, year)
    check = is_file_complete(Path(outdir, basicname + '.grib'), minimum_steps)

    if not check:
        for month in months:

            if request=='monthly':
                filename =  basicname + month + '.grib'
            elif request=='yearly':
                filename =  basicname + '.grib'

            outfile = Path(outdir, filename)

            # special feature for preliminary back extension
            #if int(year) < year_preliminary and dataset == 'ERA5':
            #    kind = kind + '-preliminary-back-extension'
            #    product_type = 'reanalysis-monthly-means-of-daily-means' # hack

            retrieve_dict = {
                'product_type': product_type,
                'format': 'grib',
                'variable': var,
                'year': year,
                'month': month,
                'day': day,
                'time': time,
            }

            if grid not in ['full']:
                # get right grid for the API call
                gridapi = grid.split('x')[0]
                retrieve_dict['grid'] = [ gridapi, gridapi ]

            if level_kind == 'pressure-levels' :
                retrieve_dict['pressure_level'] = level

            if area != 'global' :
                retrieve_dict['area'] = area

            #pprint(kind)
            #pprint(level_kind)
            #pprint(retrieve_dict)

            #retrieve data
            c = cdsapi.Client()
            
            RES = c.retrieve(
                        kind,
                        retrieve_dict,
                        )
            
            #download data
            attempt = 0

            while attempt < max_attemps:
                
                try:
                    RES.download(outfile)
                
                except IncompleteRead as e:
                    print(f"An IncompleteRead exception occurred: {e}\nRetrying the download (Attempt {attempt}/{max_attemps})")
                    attempt += 1
                    if attempt == max_attemps:
                        raise MaxAttemptsError(f"Failed to download data for year {year} after {max_attemps} attempts")


        # cat together the files and rmove the monthly ones
        if request == 'monthly':
            flist = str(Path(outdir, basicname + '??.grib'))
            cdo.cat(input = flist, output = str(Path(outdir, basicname + '.grib')))
            for f in glob.glob(flist):
                os.remove(f)


# define propertes for vertical levels
def define_level(levelout):
    if levelout == 'sfc' :
        level_kind = 'single-levels'
        level = 'sfc'
    else : 
        level_kind = 'pressure-levels'
        if levelout == 'plev8':
            level= ['10','50','100','250','500','700','850','1000']
        elif levelout == 'plev19':
            level = ['1000','925','850','700','600','500','400','300','250',
                     '200','150','100','70','50','30','20','10','5','1']
        elif levelout == 'plev37':
            level =  ['1', '2', '3', '5', '7', '10', '20', '30', '50', '70', '100', 
                      '125', '150', '175', '200', '225', '250', '300', '350', '400',
                       '450', '500', '550', '600', '650', '700', '750', '775', '800',
                        '825', '850', '875', '900', '925', '950', '975', '1000']
        elif levelout == '500hPa' :
            level = ['500']
    return level, level_kind

# define properties for time
def define_time(freq) :
    if freq == 'mon':
        time = ['00:00']
        day = ['01']
        product_type = 'monthly_averaged_reanalysis'
        time_kind = '-monthly-means'
        minimum_steps = 12
    elif freq in ['1hr','6hrs']:
        product_type = 'reanalysis'
        time_kind = ''
        day = [str(i).zfill(2) for i in range(1,31+1)]
        if freq == '6hrs':
            time = [str(i).zfill(2)+':00' for i in range(0,24,6)]
            minimum_steps = 365*4
        elif freq == '1hr':
            time = [str(i).zfill(2)+':00' for i in range(0,24)]
            minimum_steps = 365*24
    elif freq == 'instant':
        product_type = 'reanalysis'
        time_kind = ''
        day = ['01']
        time = ['00:00']
        minimum_steps = 12

    return product_type, day, time, time_kind, minimum_steps

# create filename function
def create_filename(dataset, var, freq, grid, levelout, area, year1, year2=None) :
    filename = dataset + '_' + var + '_' + freq + '_' + grid + '_' + levelout + '_' + year1
    if (freq == 'mon') and (year2 is not None):
        filename = filename + '-' + year2
    if area != 'global':
        strarea = "_".join([str(x) for x in area])
        filename = filename + '_' + strarea
    return filename

# wrapper for simple parallel function for conversion to netcdf
def year_convert(infile, outfile, debug = False) :
    cdo.debug=debug
    cdo.copy(input = str(infile), output = str(outfile), options = '-t ecmwf -f nc4 -z zip --eccodes')

# get the first and last year from files of a given folder
def first_last_year(filepattern) :
    filelist = glob.glob(str(filepattern))
    first_year=str(sorted(filelist)[0].split('_')[-1].split('.')[0])
    last_year=str(sorted(filelist)[-1].split('_')[-1].split('.')[0])
    # monthly data
    if len(first_year)>4:
        first_year = first_year.split('-',maxsplit=1)[0]
    if len(last_year)>4:
        last_year = last_year.split('-')[1]
    return first_year, last_year

# for autosearch of the missing years
def which_new_years_download(storedir, dataset, var, freq, grid, levelout, area):

    destdir = Path(storedir, var, freq)
    filepattern = Path(destdir, create_filename(dataset, var, freq, grid, 
                                                levelout, area, '????', '????') + '.nc')
    _, year1 = first_last_year(filepattern)
    year1 = int(year1) + 1
    year2 = datetime.datetime.now().year - 1
    return year1, year2

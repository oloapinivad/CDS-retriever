"""CDS-retriever module"""

import os
import sys
import glob
import re
from pathlib import Path
import datetime
import cdsapi
from cdo import Cdo, CDOException
cdo = Cdo()


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
            nt = int(cdo.ntime(input=filename, options='-s')[0])
            print(f'The file has {nt} timesteps...')

            # if the number of steps is not enough...
            if nt < minimum_steps:
                print(f'The file {filename} looks incomplete with nsteps {nt} < {minimum_steps} minimum steps')
                return False

            print(filename + ' is complete! Going to next one...')
            return True

        except (KeyError, CDOException):
            print(filename + ' is corrupted')
            return False

    # if file does not exist
    print(filename + ' is missing')
    return False

# big function for retrieval
def year_retrieve(dataset, product_type, var, freq, years_list, grid, levelout, area, outdir, request='yearly'):
    """Function to download a single year of a ERA5 dataset"""

    # Level configuration
    level, level_kind = define_level(levelout)

    if dataset == 'ERA5':
        kind = 'reanalysis-era5-' + level_kind
    elif dataset == 'ERA5-Land':
        kind = 'reanalysis-era5-land'
    else:
        raise ValueError(f'Unknown dataset {dataset} requested')

    # extract time information
    product_type_by_freq, day, time, time_kind, minimum_steps = define_time(freq, len(years_list))
    kind = kind + time_kind
    if product_type == 'reanalysis':
        product_type = product_type_by_freq

    # set up the months loop
    if request == 'yearly':
        months = [[str(i).zfill(2) for i in range(1, 12+1)]]
    elif request == 'monthly':
        months = [str(i).zfill(2) for i in range(1, 12+1)]
    else:
        sys.exit('Wrong download request!')

    # check if yearly file is complete
    basicname = create_filename(dataset, var, freq, grid, levelout, area, years_list[0], years_list[-1])
    check = is_file_complete(Path(outdir, basicname + '.grib'), minimum_steps)

    if not check:
        for month in months:

            if request == 'monthly':
                filename = basicname + month + '.grib'
            elif request == 'yearly':
                filename = basicname + '.grib'
            else:
                raise ValueError(f'Unknow request frequency {request} requested')

            outfile = Path(outdir, filename)

            # special feature for preliminary back extension
            # if int(year) < year_preliminary and dataset == 'ERA5':
            #    kind = kind + '-preliminary-back-extension'
            #    product_type = 'reanalysis-monthly-means-of-daily-means' # hack

            retrieve_dict = {
                'product_type': product_type,
                'format': 'grib',
                'variable': var,
                'year': years_list,
                'month': month,
                'day': day,
                'time': time,
            }

            if grid not in ['full']:
                # get right grid for the API call
                gridapi = grid.split('x')[0]
                retrieve_dict['grid'] = [gridapi, gridapi]

            if level_kind == 'pressure-levels':
                retrieve_dict['pressure_level'] = level

            if area != 'global':
                retrieve_dict['area'] = area

            # pprint(kind)
            # pprint(level_kind)
            print(retrieve_dict)
            # run the API
            c = cdsapi.Client()
            c.retrieve(
                kind,
                retrieve_dict,
                outfile)

        # cat together the files and rmove the monthly ones
        if request == 'monthly':
            flist = str(Path(outdir, basicname + '??.grib'))
            cdo.cat(input=flist, output=str(Path(outdir, basicname + '.grib')))
            for f in glob.glob(flist):
                os.remove(f)



plevs = {'ERA5' : ['1000', '975', '950', '925',
                    '900', '875', '850', '825',
                    '800', '775', '750', '700',
                    '650', '600', '550', '500',
                    '450', '400', '350', '300',
                    '250', '225', '200', '175',
                    '150', '125', '100',  '70',
                     '50',  '30',  '20',  '10',
                      '7',   '5',   '3',   '2',
                      '1'
                ],
        'plev8' : [  '10',  '50', '100', '250',
                    '500', '700', '850','1000'
                ],
        'plev19': ['1000', '925', '850', '700',
                    '600', '500', '400', '300',
                    '250', '200', '150', '100',
                     '70',  '50',  '30',  '20',
                     '10',   '5',   '1'
                ],
        'plev37': [   '1',   '2',   '3',   '5',
                      '7',  '10',  '20',  '30',
                     '50',  '70', '100', '125',
                    '150', '175', '200', '225',
                    '250', '300', '350', '400',
                    '450', '500', '550', '600',
                    '650', '700', '750', '775',
                    '800', '825', '850', '875',
                    '900', '925', '950', '975',
                    '1000'
                ]
}



def validate_pressure_lev(levels):
    """
    Check if the requested pressure level(s) is (are) valid ERA5 pressure level(s).
    
    Parameters:
        pressure_level (str or list of str): The pressure level(s) to be validated.
        
    Returns:
        list: A list of validated ERA5 pressure levels ready for CDS API requests.
    """

    # Check if levels is one of the predefined options
    if levels in ['plev8','plev19','plev37']:
        return plevs[levels]
    
    RES = list()

    pattern = r'^\d+(hPa)?$'

    # Convert a single str in list for consistency
    if isinstance(levels, str):
        levels = [levels]

    # Validate each level
    for l in levels:

        if re.match(pattern,l) and l.rstrip('hPa') in plevs['ERA5']:
            
            tmp = l.rstrip('hPa')
            RES.append(tmp)

        else:
            raise ValueError(f'Invalid level specification: {l}! Aborting...')

    return RES
             


def define_level(levelout):
    """
    Define properties for vertical levels based on the specified output level.

    This function determines the type of vertical level (surface or
    pressure levels) and validates the given pressure levels.

    Parameters:
        levelout (str or list of str): The output level. 
                        Accepts 'sfc' for surface levels 
                        or a (list of) string(s) for pressure levels.

    Returns:
        tuple: A tuple containing:
            - level (str): The defined level, either 'sfc'
                           or a (list of) validated pressure level(s).
            - level_kind (str): A string indicating the type of level,
                                either 'single-levels' for the surface level
                                or 'pressure-levels' for pressure levels.

    Raises:
        ValueError: If the provided levelout is not a valid pressure level.
    """
    
    if levelout == 'sfc':

        level_kind = 'single-levels'
        level = 'sfc'
    else:

        level_kind = 'pressure-levels'
        try:
            level = validate_pressure_lev(levelout)
        except ValueError as e:
            print(e)
            sys.exit(1)

    return level, level_kind



# define properties for time


def define_time(freq, n_years=1):
    """Define time frequency and provide the different request options"""
    if freq == 'mon':
        time = ['00:00']
        day = ['01']
        product_type = 'monthly_averaged_reanalysis'
        time_kind = '-monthly-means'
        minimum_steps = n_years * 12
    elif freq in ['1hr', '3hrs', '6hrs']:
        product_type = 'reanalysis'
        time_kind = ''
        day = [str(i).zfill(2) for i in range(1, 31+1)]
        if freq == '6hrs':
            time = [str(i).zfill(2)+':00' for i in range(0, 24, 6)]
            minimum_steps = n_years * 365 * 4
        elif freq == '3hrs':
            time = [str(i).zfill(2)+':00' for i in range(0, 24, 3)]
            minimum_steps = n_years * 365 * 8
        else:
            # 1hr case
            time = [str(i).zfill(2)+':00' for i in range(0, 24)]
            minimum_steps = n_years * 365 * 24
    elif freq == 'instant':
        product_type = 'reanalysis'
        time_kind = ''
        day = ['01']
        time = ['00:00']
        minimum_steps = n_years * 12
    else:
        raise ValueError(f'Unknown frequency {freq} requested')

    return product_type, day, time, time_kind, minimum_steps

# create filename function
def create_filename(dataset, var, freq, grid, levelout, area, first_year, last_year):
    """Create the final output file"""
    filename = dataset + '_' + var + '_' + freq + '_' + grid + '_' + levelout + '_' + first_year + '-' + last_year
    if area != 'global':
        strarea = "_".join([str(x) for x in area])
        filename = filename + '_' + strarea
    return filename

# wrapper for simple parallel function for conversion to netcdf
def year_convert(infile, outfile, debug=False):
    """Convert with cdo the grib to netcdf4 zip"""
    cdo.debug = debug
    cdo.copy(input=str(infile), output=str(outfile),
             options='-t ecmwf -f nc4 -z zip --eccodes')

# get the first and last year from files of a given folder
def first_last_year(filepattern):
    filelist = sorted(glob.glob(str(filepattern)))
    years = []

    for f in filelist:
        filename = Path(f).name
        core = filename.split('_')
        # search for the only part with a "-" and two numbers of 4 digits
        for part in core:
            if '-' in part:
                part_clean = part.split('.')[0]  # remove .nc extension
                a, b = part_clean.split('-', 1)
                if len(a) == 4 and len(b) == 4 and a.isdigit() and b.isdigit():
                    years.append((int(a), int(b)))
                    break

    first_year = years[0][0]
    last_year = years[-1][1]
    return str(first_year), str(last_year)

# for autosearch of the missing years
def which_new_years_download(storedir, dataset, var, freq, grid, levelout, area):
    """Identify which years we need to download if something is already found"""

    destdir = Path(storedir, var, freq)
    filepattern = Path(destdir, create_filename(dataset, var, freq, grid,
                                                levelout, area, '????', '????') + '.nc')
    print(filepattern)
    _, year1 = first_last_year(filepattern)
    year1 = int(year1) + 1
    year2 = datetime.datetime.now().year - 1
    return year1, year2

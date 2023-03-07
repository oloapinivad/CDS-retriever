import cdsapi
from pathlib import Path
import glob
from cdo import Cdo
from pprint import pprint
import datetime
cdo = Cdo()

# check with cdo is the file is complete (approximately correct)
def is_file_complete(filename, minimum_steps) : 
    filename = str(filename)
    try : 
        out = cdo.ntime(input=filename, options = '-s')
        # this is an hack due to warning being reported by cdo into the output!
        for n in out : 
            if (len(n) <= 5) :
                nt = n  
    except : 
        print(filename + ' is missing')
        nt = 0 
    print(nt)
    if (int(nt) < minimum_steps) :
        print('Need to retrieve ' + filename)
        retrieve = True
    else :
        print(filename + ' is complete! Going to next one...')
        retrieve = False
    return retrieve

    
# big function for retrieval
def year_retrieve(var, freq, year, grid, levelout, area, outdir) : 

    # year for preliminary era5 reanalysis
    year_preliminary = 1959

    # configuration part (level)
    level, level_kind = define_level(levelout)
    kind = 'reanalysis-era5-' + level_kind 

    product_type, day, time, time_kind, minimum_steps = define_time(freq)
    kind = kind + time_kind

    months = [str(i).zfill(2) for i in range(1,12+1)]

    filename =  create_filename(var, freq, grid, levelout, area, year) + '.grib'
    outfile = Path(outdir, filename)

    run_year = is_file_complete(outfile, minimum_steps) 
    if run_year : 
    
        # special feature for preliminary back extension
        if int(year) < year_preliminary :
            kind = kind + '-preliminary-back-extension'
            product_type = 'reanalysis-monthly-means-of-daily-means' # hack

        # check what I am making up
        #biglist = ['kind', 'product_type', 'var', 'year', 'freq', 'months', 'day', 'time', 'levelout', 'level', 'grid', 'outfile']
        #for test in biglist : 
        #    if isinstance(locals()[test], list):
        #        print(test + ': ' + ' '.join(locals()[test]))
        #    else : 
        #        print(test + ': ' + str(locals()[test]))

        # get right grid for the API call
        gridapi = grid.split('x')[0]

        retrieve_dict = {
            'product_type': product_type,
            'format': 'grib',
            'variable': var,
            'year': year,
            'month': months,
            'day': day,
            'time': time,
            'grid': [ gridapi, gridapi ]
        }

        if level_kind == 'pressure_level' :
            retrieve_dict['pressure_level'] = level

        if area != 'global' :
            retrieve_dict['area'] = area

        pprint(kind)
        pprint(retrieve_dict)
        # run the API
        c = cdsapi.Client()
        c.retrieve(
            kind,
            retrieve_dict,
            outfile)

# define propertes for vertical levels
def define_level(levelout) : 
    if levelout == 'sfc' :
        level_kind = 'single-levels'
        level = 'sfc'
    else : 
        level_kind = 'pressure-levels'
        if levelout == 'plev8' :
            level= ['10','50','100','250','500','700','850','1000']
        elif levelout == 'plev19' :
            level = ['1000','925','850','700','600','500','400','300','250','200','150','100','70','50','30','20','10','5','1']
        elif levelout == 'plev37' : 
            level =  ['1', '2', '3', '5', '7', '10', '20', '30', '50', '70', '100', '125', '150', '175', 
                '200', '225', '250', '300', '350', '400', '450', '500', '550', '600', '650', '700', '750',
                '775', '800', '825', '850', '875', '900', '925', '950', '975', '1000']
        elif levelout == '500hPa' :
            level = ['500']
    return level, level_kind

# define properties for time 
def define_time(freq) :
    if freq == 'mon' :
        time = ['00:00']
        day = ['01']
        product_type = 'monthly_averaged_reanalysis'
        time_kind = '-monthly-means'
        minimum_steps = 12
    elif freq in ['1hr','6hrs'] : 
        product_type = 'reanalysis'
        time_kind = ''
        day = [str(i).zfill(2) for i in range(1,31+1)]
        if freq == '6hrs' :
            time = [str(i).zfill(2)+':00' for i in range(0,24,6)]
            minimum_steps = 365*4
        elif freq == '1hr' :
            time = [str(i).zfill(2)+':00' for i in range(0,24)]
            minimum_steps = 365*24
    elif freq == 'instant' : 
        product_type = 'reanalysis'
        time_kind = ''
        day = ['01']
        time = ['00:00']
        minimum_steps = 12

    return product_type, day, time, time_kind, minimum_steps

# create filename function
def create_filename(var, freq, grid, levelout, area, year1, year2=None) :
    filename = 'ERA5_' + var + '_' + freq + '_' + grid + '_' + levelout + '_' + year1
    if (freq == 'mon') and (year2 is not None):
        filename = filename + '-' + year2
    if area != 'global' : 
        strarea = "_".join([str(x) for x in area])
        filename = filename + '_' + strarea
    return(filename)

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
        first_year = first_year.split('-')[0]
    if len(last_year)>4:
        last_year = last_year.split('-')[1]
    return first_year, last_year


def which_new_years_download(storedir, var, freq, grid, levelout, area):

    destdir = Path(storedir, var, freq)
    filepattern = Path(destdir, create_filename(var, freq, grid, levelout, area, '????', '????') + '.nc')
    _, year1 = first_last_year(filepattern)
    year1 = int(year1) + 1
    year2 = datetime.datetime.now().year - 1
    return year1, year2

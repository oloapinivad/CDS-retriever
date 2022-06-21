import cdsapi
import os

 # big function for retrieval
def year_retrieve(var, freq, year, grid, levelout, outdir) : 

    # configuration part (level)
    level, level_kind = define_level(levelout)
    kind = 'reanalysis-era5-' + level_kind 

    product_type, day, time, time_kind = define_time(freq)
    kind = kind + time_kind

    months = [str(i).zfill(2) for i in range(1,12+1)]

    filename = 'ERA5_' + var + '_' + freq + '_' + grid + '_' + levelout + '_' + year + '.grib'
    outfile = os.path.join(outdir, filename)


    # special feature for preliminary back extension
    if int(year) < 1959 :
        kind = kind + 'preliminary-back-extension'

     # check what I am making up
    biglist = ['kind', 'product_type', 'var', 'year', 'freq', 'months', 'day', 'time', 'levelout', 'level', 'grid', 'outfile']
    for test in biglist : 
        if isinstance(locals()[test], list):
            print(test + ': ' + ' '.join(locals()[test]))
        else : 
            print(test + ': ' + locals()[test])

    # get right grid for the API call
    gridapi = grid.split('x')[0]
    
    # run the API
    c = cdsapi.Client()
    c.retrieve(
        kind,
            {
            'product_type': product_type,
            'format': 'grib',
            'variable': var,
            'pressure_level': level,
            'year': year,
            'month': months,
            'day': day,
            'time': time,
            'grid': [ gridapi, gridapi ]
        },
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
    else : 
        product_type = 'reanalysis'
        time_kind = ''
        day = [str(i).zfill(2) for i in range(1,31+1)]
        if freq == '6hrs' :
            time = [str(i).zfill(2)+':00' for i in range(0,24,6)]
        elif freq == '1hr' :
            time = [str(i).zfill(2)+':00' for i in range(0,24)]

    return product_type, day, time, time_kind
"""Build CDS API requests and file names for ERA5 / ERA5-Land.

Everything in this module is pure python: no CDO, no network.
"""

import calendar
import re

DATASETS = ('ERA5', 'ERA5-Land')
FREQS = ('instant', '1hr', '3hrs', '6hrs', 'mon')
GRIDS = ('full', '0.1x0.1', '0.25x0.25', '0.5x0.5', '1x1', '2.5x2.5')
PRODUCT_TYPES = ('reanalysis', 'ensemble_members', 'ensemble_mean', 'ensemble_spread')
REQUEST_MODES = ('auto', 'yearly', 'monthly')

# hours between two consecutive steps for sub-daily frequencies
HOURS_STEP = {'1hr': 1, '3hrs': 3, '6hrs': 6}

PLEVS = {
    'ERA5': ['1', '2', '3', '5', '7', '10', '20', '30', '50', '70', '100', '125',
             '150', '175', '200', '225', '250', '300', '350', '400', '450', '500',
             '550', '600', '650', '700', '750', '775', '800', '825', '850', '875',
             '900', '925', '950', '975', '1000'],
    'plev8': ['10', '50', '100', '250', '500', '700', '850', '1000'],
    'plev19': ['1000', '925', '850', '700', '600', '500', '400', '300', '250',
               '200', '150', '100', '70', '50', '30', '20', '10', '5', '1'],
}
PLEVS['plev37'] = PLEVS['ERA5']

_LEVEL_PATTERN = re.compile(r'^(\d+)(hPa)?$')


def validate_pressure_lev(levels):
    """
    Check if the requested pressure level(s) are valid ERA5 pressure levels.

    Parameters:
        levels (str or list of str): 'plev8', 'plev19', 'plev37', a single level
            ('500' or '500hPa') or a list of single levels.

    Returns:
        list: validated pressure levels (str, without unit) for the CDS API.

    Raises:
        ValueError: if any level is not an ERA5 pressure level.
    """
    if isinstance(levels, str) and levels in ('plev8', 'plev19', 'plev37'):
        return list(PLEVS[levels])

    if isinstance(levels, (str, int)):
        levels = [levels]

    validated = []
    for level in levels:
        match = _LEVEL_PATTERN.match(str(level))
        if not match or match.group(1) not in PLEVS['ERA5']:
            raise ValueError(f'Invalid pressure level specification: {level}')
        validated.append(match.group(1))
    return validated


def define_level(levelout):
    """
    Define properties for vertical levels.

    Parameters:
        levelout (str or list of str): 'sfc', a predefined set ('plev8', 'plev19',
            'plev37'), a single pressure level ('500hPa') or a list of levels.

    Returns:
        tuple: (levels, level_kind) where levels is None for 'sfc' or the list of
            validated pressure levels, and level_kind is 'single-levels' or 'pressure-levels'.
    """
    if levelout == 'sfc':
        return None, 'single-levels'
    return validate_pressure_lev(levelout), 'pressure-levels'


def level_label(levelout):
    """
    String used for levelout in file names.

    'sfc', 'plev19' and '500hPa' are kept as they are,
    a list of levels becomes e.g. '500-700-850hPa'.
    """
    if isinstance(levelout, (list, tuple)):
        if len(levelout) == 1:
            return level_label(levelout[0])
        return '-'.join(validate_pressure_lev(levelout)) + 'hPa'
    levelout = str(levelout)
    if _LEVEL_PATTERN.match(levelout) and not levelout.endswith('hPa'):
        return levelout + 'hPa'
    return levelout


def dataset_id(dataset, level_kind, freq):
    """Return the CDS dataset (process) identifier"""
    if dataset == 'ERA5':
        name = 'reanalysis-era5-' + level_kind
    elif dataset == 'ERA5-Land':
        if level_kind != 'single-levels':
            raise ValueError('ERA5-Land provides only surface data: use levelout: sfc')
        name = 'reanalysis-era5-land'
    else:
        raise ValueError(f'Unknown dataset {dataset} requested, options are {DATASETS}')

    if freq == 'mon':
        name = name + '-monthly-means'
    return name


def define_time(freq, product_type='reanalysis'):
    """
    Define time selection for a given frequency.

    Returns:
        tuple: (product_type, day, time), day is None for monthly means
            since that dataset has no day selection.
    """
    if freq not in FREQS:
        raise ValueError(f'Unknown frequency {freq} requested, options are {FREQS}')

    if product_type not in PRODUCT_TYPES:
        raise ValueError(f'Unknown product_type {product_type}, options are {PRODUCT_TYPES}')

    if freq == 'mon':
        if product_type == 'reanalysis':
            product_type = 'monthly_averaged_reanalysis'
        elif product_type == 'ensemble_members':
            product_type = 'monthly_averaged_ensemble_members'
        else:
            raise ValueError(f'product_type {product_type} not available for monthly means')
        return product_type, None, ['00:00']

    if product_type != 'reanalysis' and freq == '1hr':
        raise ValueError('ERA5 ensemble products are 3-hourly: use 3hrs or 6hrs')

    days = [str(i).zfill(2) for i in range(1, 32)]
    if freq == 'instant':
        return product_type, ['01'], ['00:00']

    step = HOURS_STEP[freq]
    return product_type, days, [str(i).zfill(2) + ':00' for i in range(0, 24, step)]


def expected_steps(freq, year, months):
    """Number of time steps expected in a GRIB file covering the given months of a year"""
    if freq in ('mon', 'instant'):
        return len(months)
    ndays = sum(calendar.monthrange(int(year), int(m))[1] for m in months)
    return ndays * 24 // HOURS_STEP[freq]


def month_chunks(request_mode):
    """List of month chunks: one chunk for yearly requests, twelve for monthly ones"""
    months = [str(i).zfill(2) for i in range(1, 13)]
    if request_mode == 'yearly':
        return [months]
    if request_mode == 'monthly':
        return [[m] for m in months]
    raise ValueError(f'Unknown download request {request_mode}, options are yearly/monthly')


def build_request(dataset, var, freq, years, months, grid='full', levelout='sfc',
                  area='global', product_type='reanalysis'):
    """
    Build a CDS API request following the current (2024+) CDS interface.

    Parameters:
        years, months: str or list of str

    Returns:
        tuple: (dataset identifier, request dictionary)
    """
    levels, level_kind = define_level(levelout)
    name = dataset_id(dataset, level_kind, freq)
    product_type, day, time = define_time(freq, product_type)

    as_list = lambda x: [str(v) for v in x] if isinstance(x, (list, tuple)) else [str(x)]

    request = {}
    # ERA5-Land hourly has no product type selection
    if name != 'reanalysis-era5-land':
        request['product_type'] = [product_type]
    request['variable'] = as_list(var)
    request['year'] = as_list(years)
    request['month'] = as_list(months)
    if day is not None:
        request['day'] = day
    request['time'] = time
    if levels is not None:
        request['pressure_level'] = levels
    if grid != 'full':
        # 'grid' is not in the web form, but it is still honoured by the MARS backend
        gridapi = grid.split('x')[0]
        request['grid'] = [gridapi, gridapi]
    if area != 'global':
        request['area'] = [float(x) for x in area]
    request['data_format'] = 'grib'
    request['download_format'] = 'unarchived'

    return name, request


def create_filename(dataset, var, freq, grid, levelout, area, year1, year2=None):
    """
    Create the output file name (without extension):
    <dataset>_<var>_<freq>_<grid>_<levelout>_<year1>[-<year2>][_<area>]

    year2 is appended only for monthly data (merged multi-year files).
    """
    filename = '_'.join([dataset, var, freq, grid, level_label(levelout), str(year1)])
    if freq == 'mon' and year2 is not None:
        filename = filename + '-' + str(year2)
    if area != 'global':
        filename = filename + '_' + '_'.join(str(x) for x in area)
    return filename

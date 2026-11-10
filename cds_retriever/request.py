"""Build CDS API requests and file names for ERA5 / ERA5-Land / ERA5-HEAT.

Everything in this module is pure python: no CDO, no network.
"""

import calendar
import re

DATASETS = ('ERA5', 'ERA5-Land', 'ERA5-HEAT')
FREQS = ('instant', '1hr', '3hrs', '6hrs', 'day', 'daymin', 'daymax', 'daysum', 'mon')
GRIDS = ('full', '0.1x0.1', '0.25x0.25', '0.5x0.5', '1x1', '2.5x2.5')
PRODUCT_TYPES = ('reanalysis', 'ensemble_members', 'ensemble_mean', 'ensemble_spread')
REQUEST_MODES = ('auto', 'yearly', 'monthly')

# hours between two consecutive steps for sub-daily frequencies
HOURS_STEP = {'1hr': 1, '3hrs': 3, '6hrs': 6}

# daily statistics computed by the CDS from the hourly data, in UTC days
# (derived-era5-*-daily-statistics datasets, delivered as netCDF)
DAILY_STATISTICS = {'day': 'daily_mean', 'daymin': 'daily_minimum', 'daymax': 'daily_maximum',
                    'daysum': 'daily_sum'}
DAILY_PRODUCT_TYPES = ('reanalysis', 'ensemble_members', 'ensemble_mean')

# ERA5-HEAT thermal comfort indices (derived-utci-historical): hourly fields on the 0.25 degree
# grid, or the daily minimum and maximum of the UTCI, delivered as a zip of daily netCDF files.
# The consolidated dataset covers the complete years, the intermediate one only the last months.
HEAT_DATASET = 'derived-utci-historical'
HEAT_VERSION = '1_1'
HEAT_VARIABLES = ('universal_thermal_climate_index', 'mean_radiant_temperature')
# netCDF variable of each daily statistic
HEAT_DAILY = {'daymin': 'utci_daily_min', 'daymax': 'utci_daily_max'}

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

# ecCodes paramId of the ERA5 variables (CDS names), used to split requests with several
# variables into one file per variable. paramIds do not change across ecCodes versions,
# unlike short names (e.g. sea ice cover: 'ci' or 'siconc').
# Variables not listed here are always downloaded with their own request.
PARAM_IDS = {
    # single levels
    '10m_u_component_of_wind': 165,
    '10m_v_component_of_wind': 166,
    '10m_wind_speed': 207,
    '2m_dewpoint_temperature': 168,
    '2m_temperature': 167,
    'boundary_layer_height': 159,
    'convective_precipitation': 143,
    'eastward_turbulent_surface_stress': 180,
    'evaporation': 182,
    'high_cloud_cover': 188,
    'land_sea_mask': 172,
    'large_scale_precipitation': 142,
    'low_cloud_cover': 186,
    'mean_sea_level_pressure': 151,
    'medium_cloud_cover': 187,
    'northward_turbulent_surface_stress': 181,
    'runoff': 205,
    'sea_ice_cover': 31,
    'sea_surface_temperature': 34,
    'skin_temperature': 235,
    'snow_depth': 141,
    'snowfall': 144,
    'soil_temperature_level_1': 139,
    'soil_temperature_level_2': 170,
    'soil_temperature_level_3': 183,
    'soil_temperature_level_4': 236,
    'surface_latent_heat_flux': 147,
    'surface_net_solar_radiation': 176,
    'surface_net_solar_radiation_clear_sky': 210,
    'surface_net_thermal_radiation': 177,
    'surface_net_thermal_radiation_clear_sky': 211,
    'surface_pressure': 134,
    'surface_sensible_heat_flux': 146,
    'surface_solar_radiation_downwards': 169,
    'surface_thermal_radiation_downwards': 175,
    'toa_incident_solar_radiation': 212,
    'top_net_solar_radiation': 178,
    'top_net_solar_radiation_clear_sky': 208,
    'top_net_thermal_radiation': 179,
    'top_net_thermal_radiation_clear_sky': 209,
    'total_cloud_cover': 164,
    'total_column_ozone': 206,
    'total_column_water': 136,
    'total_column_water_vapour': 137,
    'total_precipitation': 228,
    'total_sky_direct_solar_radiation_at_surface': 228021,
    'volumetric_soil_water_layer_1': 39,
    'volumetric_soil_water_layer_2': 40,
    'volumetric_soil_water_layer_3': 41,
    'volumetric_soil_water_layer_4': 42,
    # pressure levels
    'divergence': 155,
    'fraction_of_cloud_cover': 248,
    'geopotential': 129,
    'ozone_mass_mixing_ratio': 203,
    'potential_vorticity': 60,
    'relative_humidity': 157,
    'specific_cloud_ice_water_content': 247,
    'specific_cloud_liquid_water_content': 246,
    'specific_humidity': 133,
    'temperature': 130,
    'u_component_of_wind': 131,
    'v_component_of_wind': 132,
    'vertical_velocity': 135,
    'vorticity': 138,
}

# Accumulated variables: the first step of the dataset (00:00 of 1 January of its first year)
# does not exist, because the accumulation starts after it
ACCUMULATED_VARIABLES = {
    'convective_precipitation', 'eastward_turbulent_surface_stress', 'evaporation',
    'evaporation_from_bare_soil', 'evaporation_from_open_water_surfaces_excluding_oceans',
    'evaporation_from_the_top_of_canopy', 'evaporation_from_vegetation_transpiration',
    'large_scale_precipitation', 'northward_turbulent_surface_stress', 'potential_evaporation',
    'runoff', 'snow_evaporation', 'snowfall', 'snowmelt', 'sub_surface_runoff',
    'surface_latent_heat_flux', 'surface_net_solar_radiation',
    'surface_net_solar_radiation_clear_sky', 'surface_net_thermal_radiation',
    'surface_net_thermal_radiation_clear_sky', 'surface_runoff', 'surface_sensible_heat_flux',
    'surface_solar_radiation_downwards', 'surface_thermal_radiation_downwards',
    'toa_incident_solar_radiation', 'top_net_solar_radiation', 'top_net_solar_radiation_clear_sky',
    'top_net_thermal_radiation', 'top_net_thermal_radiation_clear_sky', 'total_evaporation',
    'total_precipitation', 'total_sky_direct_solar_radiation_at_surface',
}
# first year of the datasets
FIRST_YEAR = {'ERA5': 1940, 'ERA5-Land': 1950}
# ERA5-Land starts at 01:00 of 1 January 1950 also for the instantaneous variables (2m_temperature)
NO_FIRST_STEP = ('ERA5-Land',)

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

    A string ('sfc', 'plev19', '500hPa', '500') is used verbatim, as in the
    original naming scheme, so that existing archives can still be updated.
    A list of levels becomes e.g. '500-700-850hPa'.
    """
    if isinstance(levelout, (list, tuple)):
        if len(levelout) == 1:
            return str(levelout[0])
        return '-'.join(validate_pressure_lev(levelout)) + 'hPa'
    return str(levelout)


def is_daily(freq):
    """True for the daily statistics computed by the CDS"""
    return freq in DAILY_STATISTICS


def download_suffix(freq, dataset='ERA5'):
    """Extension of the downloaded files: daily statistics and ERA5-HEAT are netCDF only"""
    return '.nc' if is_daily(freq) or dataset == 'ERA5-HEAT' else '.grib'


def heat_variable(var, freq):
    """
    ERA5-HEAT variable to request, and netCDF variable to select in the downloaded files
    (None: the single variable of the files)
    """
    if var not in HEAT_VARIABLES:
        raise ValueError(f'Unknown ERA5-HEAT variable {var}, options are {HEAT_VARIABLES}')
    if freq == '1hr':
        return var, None
    if freq in HEAT_DAILY:
        if var != 'universal_thermal_climate_index':
            raise ValueError(f'ERA5-HEAT daily statistics ({freq}) are available only for '
                             'universal_thermal_climate_index')
        return 'universal_thermal_climate_index_daily_statistics', HEAT_DAILY[freq]
    raise ValueError(f'ERA5-HEAT is available with freq 1hr, {", ".join(HEAT_DAILY)}, not {freq}')


def dataset_id(dataset, level_kind, freq):
    """Return the CDS dataset (process) identifier"""
    if dataset == 'ERA5':
        name = 'reanalysis-era5-' + level_kind
    elif dataset == 'ERA5-Land':
        if level_kind != 'single-levels':
            raise ValueError('ERA5-Land provides only surface data: use levelout: sfc')
        name = 'reanalysis-era5-land'
    elif dataset == 'ERA5-HEAT':
        if level_kind != 'single-levels':
            raise ValueError('ERA5-HEAT provides only surface data: use levelout: sfc')
        if freq != '1hr' and freq not in HEAT_DAILY:
            raise ValueError(f'ERA5-HEAT is available with freq 1hr, '
                             f'{", ".join(HEAT_DAILY)}, not {freq}')
        return HEAT_DATASET
    else:
        raise ValueError(f'Unknown dataset {dataset} requested, options are {DATASETS}')

    if is_daily(freq):
        if freq == 'daysum' and level_kind != 'single-levels':
            raise ValueError('daily sums (daysum) are available only on single levels')
        return 'derived-' + name.removeprefix('reanalysis-') + '-daily-statistics'
    if freq == 'mon':
        name = name + '-monthly-means'
    return name


def define_time(freq, product_type='reanalysis'):
    """
    Define time selection for a given frequency.

    Returns:
        tuple: (product_type, day, time), day is None for monthly means
            since that dataset has no day selection, time is None for daily statistics.
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

    days = [str(i).zfill(2) for i in range(1, 32)]
    if is_daily(freq):
        if product_type not in DAILY_PRODUCT_TYPES:
            raise ValueError(f'product_type {product_type} not available for daily statistics, '
                             f'options are {DAILY_PRODUCT_TYPES}')
        return product_type, days, None

    if product_type != 'reanalysis' and freq == '1hr':
        raise ValueError('ERA5 ensemble products are 3-hourly: use 3hrs or 6hrs')

    if freq == 'instant':
        return product_type, ['01'], ['00:00']

    step = HOURS_STEP[freq]
    return product_type, days, [str(i).zfill(2) + ':00' for i in range(0, 24, step)]


def expected_steps(freq, year, months, dataset=None, var=None):
    """
    Number of time steps expected in a GRIB file covering the given months of a year.

    With dataset and var (a name or a list of names), the first step of the dataset is not
    expected for accumulated sub-daily variables, nor for any variable of ERA5-Land.
    """
    if freq in ('mon', 'instant'):
        return len(months)
    ndays = sum(calendar.monthrange(int(year), int(m))[1] for m in months)
    if is_daily(freq):
        return ndays
    steps = ndays * 24 // HOURS_STEP[freq]
    names = [var] if isinstance(var, str) else (var or [])
    if (int(year) == FIRST_YEAR.get(dataset) and '01' in months
            and (dataset in NO_FIRST_STEP or any(name in ACCUMULATED_VARIABLES for name in names))):
        steps -= 1
    return steps


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
    Daily statistics are computed from the hourly data in UTC days, and delivered as netCDF.

    Parameters:
        years, months: str or list of str

    Returns:
        tuple: (dataset identifier, request dictionary)
    """
    as_list = lambda x: [str(v) for v in x] if isinstance(x, (list, tuple)) else [str(x)]
    levels, level_kind = define_level(levelout)
    name = dataset_id(dataset, level_kind, freq)
    if dataset == 'ERA5-HEAT':
        return name, heat_request(var, freq, as_list(years), as_list(months), grid, area)
    product_type, day, time = define_time(freq, product_type)

    request = {}
    # ERA5-Land hourly and daily statistics have no product type selection
    if name not in ('reanalysis-era5-land', 'derived-era5-land-daily-statistics'):
        request['product_type'] = [product_type]
    request['variable'] = as_list(var)
    request['year'] = as_list(years)
    request['month'] = as_list(months)
    if day is not None:
        request['day'] = day
    if time is not None:
        request['time'] = time
    if is_daily(freq):
        request['daily_statistic'] = DAILY_STATISTICS[freq]
        request['time_zone'] = 'utc+00:00'
        request['frequency'] = '1_hourly'
    if levels is not None:
        request['pressure_level'] = levels
    if grid != 'full':
        if is_daily(freq):
            raise ValueError('grid selection is not available for daily statistics: use grid: full')
        # 'grid' is not in the web form, but it is still honoured by the MARS backend
        gridapi = grid.split('x')[0]
        request['grid'] = [gridapi, gridapi]
    if area != 'global':
        request['area'] = [float(x) for x in area]
    if not is_daily(freq):
        request['data_format'] = 'grib'
        request['download_format'] = 'unarchived'

    return name, request


def heat_request(var, freq, years, months, grid='full', area='global'):
    """Request to ERA5-HEAT (derived-utci-historical): consolidated dataset, all the days"""
    if grid != 'full':
        raise ValueError('grid selection is not available for ERA5-HEAT: use grid: full')
    variables = [heat_variable(v, freq)[0] for v in ([var] if isinstance(var, str) else var)]
    request = {'variable': variables, 'version': HEAT_VERSION,
               'product_type': 'consolidated_dataset', 'year': years, 'month': months,
               'day': [str(i).zfill(2) for i in range(1, 32)]}
    if area != 'global':
        request['area'] = [float(x) for x in area]
    return request


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

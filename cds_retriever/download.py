"""Download ERA5 data from the CDS"""

import logging
import time
from pathlib import Path

from .api import estimate_cost
from .cdo_utils import get_cdo, ntime
from .request import build_request, create_filename, expected_steps, month_chunks

log = logging.getLogger(__name__)


def is_file_complete(filename, minimum_steps):
    """
    Check (approximately) whether a GRIB/netCDF file is complete.

    Returns:
        bool: True if the file exists and has at least minimum_steps time steps.
    """
    filename = Path(filename)
    if not filename.exists():
        log.info('%s is missing', filename)
        return False

    nt = ntime(filename)
    if nt is None:
        log.warning('%s is corrupted', filename)
        return False
    if nt < minimum_steps:
        log.warning('%s looks incomplete with %d < %d steps', filename, nt, minimum_steps)
        return False

    log.info('%s is complete with %d steps', filename, nt)
    return True


def choose_request_mode(dataset, request):
    """
    Decide between yearly and monthly requests from the CDS cost estimate.

    Parameters:
        dataset, request: a yearly request (12 months)

    Returns:
        str: 'yearly' or 'monthly'
    """
    try:
        cost, limit = estimate_cost(dataset, request)
    except Exception as exc:  # network issues should not block the download
        log.warning('Cannot estimate request cost (%s), falling back to monthly requests', exc)
        return 'monthly'

    log.info('Yearly request cost %d, limit %d', cost, limit)
    if cost <= limit:
        return 'yearly'

    monthly = dict(request, month=['01'])
    cost, limit = estimate_cost(dataset, monthly)
    if cost > limit:
        raise ValueError(f'Even a monthly request is too large ({cost} > {limit}): '
                         'reduce the number of levels, the area or the frequency')
    return 'monthly'


def get_client(**kwargs):
    """Create a cdsapi client (imported lazily to keep the module importable without it)"""
    import cdsapi
    return cdsapi.Client(**kwargs)


def download(dataset, request, target, retries=3, wait=60, client=None):
    """
    Retrieve a request into target, retrying on failure.

    Data are written to a temporary '.part' file and moved to target only once
    the download is complete, so that interrupted downloads are never mistaken
    for valid files.
    """
    target = Path(target)
    part = target.with_name(target.name + '.part')
    client = client or get_client()

    for attempt in range(1, retries + 1):
        try:
            log.info('Requesting %s (attempt %d/%d): %s', target.name, attempt, retries, request)
            client.retrieve(dataset, request, str(part))
            part.replace(target)
            return target
        except Exception as exc:
            part.unlink(missing_ok=True)
            if attempt == retries:
                raise RuntimeError(f'Failed to download {target.name} after {retries} attempts: {exc}') from exc
            log.warning('Download of %s failed: %s. Retrying in %d s', target.name, exc, wait * attempt)
            time.sleep(wait * attempt)
    return target


def year_file(outdir, dataset, var, freq, grid, levelout, area, year):
    """Path of the yearly GRIB file"""
    return Path(outdir, create_filename(dataset, var, freq, grid, levelout, area, year) + '.grib')


def month_file(yearfile, month):
    """Path of a monthly GRIB chunk of a yearly file"""
    yearfile = Path(yearfile)
    return yearfile.with_name(f'{yearfile.stem}_month{month}.grib')


def is_year_complete(outdir, dataset, var, freq, grid, levelout, area, year):
    """True if the yearly GRIB file is already on disk and complete"""
    yearfile = year_file(outdir, dataset, var, freq, grid, levelout, area, year)
    return is_file_complete(yearfile, expected_steps(freq, year, month_chunks('yearly')[0]))


def chunk_retrieve(dataset, var, freq, year, months, grid, levelout, area, outfile,
                   product_type='reanalysis', retries=3, client=None):
    """
    Download the given months of a year into outfile, unless it is already complete.

    Returns:
        Path: outfile
    """
    outfile = Path(outfile)
    minimum_steps = expected_steps(freq, year, months)
    if is_file_complete(outfile, minimum_steps):
        return outfile

    name, req = build_request(dataset, var, freq, str(year), months, grid=grid, levelout=levelout,
                              area=area, product_type=product_type)
    download(name, req, outfile, retries=retries, client=client or get_client())

    if not is_file_complete(outfile, minimum_steps):
        raise RuntimeError(f'Downloaded file {outfile} is incomplete')
    return outfile


def month_retrieve(dataset, var, freq, year, month, grid, levelout, area, outdir,
                   product_type='reanalysis', retries=3):
    """
    Download a single month of a year as a GRIB chunk '<basicname>_monthMM.grib'.
    Chunks are later joined by concat_months.

    Returns:
        Path: the monthly GRIB file
    """
    yearfile = year_file(outdir, dataset, var, freq, grid, levelout, area, year)
    return chunk_retrieve(dataset, var, freq, year, [month], grid, levelout, area,
                          month_file(yearfile, month), product_type=product_type, retries=retries)


def concat_months(dataset, var, freq, year, grid, levelout, area, outdir):
    """
    Concatenate the twelve monthly chunks of a year into the yearly GRIB file
    and remove them.

    Returns:
        Path: the yearly GRIB file
    """
    yearfile = year_file(outdir, dataset, var, freq, grid, levelout, area, year)
    monthly_files = [month_file(yearfile, m[0]) for m in month_chunks('monthly')]
    missing = [f.name for f in monthly_files if not f.exists()]
    if missing:
        raise RuntimeError(f'Cannot build {yearfile.name}, missing chunks: {", ".join(missing)}')

    tmpfile = yearfile.with_name(yearfile.name + '.part')
    get_cdo().cat(input=' '.join(str(f) for f in monthly_files), output=str(tmpfile))
    if not is_file_complete(tmpfile, expected_steps(freq, year, month_chunks('yearly')[0])):
        tmpfile.unlink(missing_ok=True)
        raise RuntimeError(f'Concatenated file {yearfile.name} is incomplete')
    tmpfile.replace(yearfile)
    for f in monthly_files:
        f.unlink()
    return yearfile


def year_retrieve(dataset, var, freq, year, grid, levelout, area, outdir,
                  request='yearly', product_type='reanalysis', retries=3):
    """
    Download a single year of an ERA5 / ERA5-Land dataset as a GRIB file.

    With request='monthly' the twelve months are downloaded sequentially and then
    concatenated (the CLI instead runs the months in parallel with month_retrieve
    and concat_months). Complete files already on disk are not downloaded again.

    Returns:
        Path: the yearly GRIB file
    """
    year = str(year)
    yearfile = year_file(outdir, dataset, var, freq, grid, levelout, area, year)
    if is_year_complete(outdir, dataset, var, freq, grid, levelout, area, year):
        return yearfile

    client = get_client()
    if request == 'yearly':
        return chunk_retrieve(dataset, var, freq, year, month_chunks('yearly')[0], grid, levelout,
                              area, yearfile, product_type=product_type, retries=retries, client=client)

    for (month,) in month_chunks(request):
        chunk_retrieve(dataset, var, freq, year, [month], grid, levelout, area,
                       month_file(yearfile, month), product_type=product_type, retries=retries,
                       client=client)
    return concat_months(dataset, var, freq, year, grid, levelout, area, outdir)

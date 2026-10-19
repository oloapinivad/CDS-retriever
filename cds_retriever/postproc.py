"""Postprocessing with CDO: GRIB to netCDF conversion, merging, time alignment, daily means"""

import datetime
import glob
import logging
import re
from pathlib import Path

from .cdo_utils import get_cdo
from .request import create_filename

log = logging.getLogger(__name__)

NC_OPTIONS = '-f nc4 -z zip'
_YEARS_RE = re.compile(r'^(\d{4})(?:-(\d{4}))?')


def year_convert(infile, outfile, debug=False):
    """Convert with CDO (ecCodes backend) a GRIB file to compressed netCDF4"""
    outfile = Path(outfile)
    tmpfile = outfile.with_name(outfile.name + '.part')
    cdo = get_cdo()
    cdo.debug = debug
    cdo.copy(input=str(infile), output=str(tmpfile), options=NC_OPTIONS + ' --eccodes')
    tmpfile.replace(outfile)
    return outfile


def year_range(files, dataset, var, freq, grid, levelout):
    """
    First and last year covered by a list of files following the naming convention
    of create_filename (either single years or year1-year2 ranges).
    """
    prefix = create_filename(dataset, var, freq, grid, levelout, 'global', '')
    first, last = [], []
    for f in files:
        name = Path(f).name
        match = _YEARS_RE.match(name[len(prefix):]) if name.startswith(prefix) else None
        if match:
            first.append(match.group(1))
            last.append(match.group(2) or match.group(1))
    if not first:
        raise FileNotFoundError(f'No files matching {prefix}<year> found')
    return min(first), max(last)


def find_files(destdir, dataset, var, freq, grid, levelout, area, merged=False):
    """Sorted list of yearly (or merged, year1-year2) netCDF files in destdir"""
    years = ('????', '????') if merged else ('????',)
    pattern = Path(destdir, create_filename(dataset, var, freq, grid, levelout, area, *years) + '.nc')
    return sorted(glob.glob(str(pattern)))


def find_legacy_area_files(destdir, dataset, var, freq, grid, levelout, area):
    """
    Merged files written by versions before 0.2 with an area selection: their year
    range was parsed from the area string, e.g. ..._40-40_70_-20_30_40.nc
    """
    if area == 'global':
        return []
    last = str(area[-1])
    pattern = create_filename(dataset, var, freq, grid, levelout, area, f'{last}-{last}') + '.nc'
    return [Path(f).name for f in sorted(glob.glob(str(Path(destdir, glob.escape(pattern)))))]


def which_new_years_download(storedir, dataset, var, freq, grid, levelout, area):
    """
    Identify which years need to be downloaded to extend an existing dataset:
    from the year after the last one available to the last complete year.
    """
    destdir = Path(storedir, var, freq)
    files = (find_files(destdir, dataset, var, freq, grid, levelout, area, merged=True) +
             find_files(destdir, dataset, var, freq, grid, levelout, area))
    if not files:
        msg = f'Update requested, but no existing files for {var} found in {destdir}'
        legacy = find_legacy_area_files(destdir, dataset, var, freq, grid, levelout, area)
        if legacy:
            msg += ('. Found files with wrong years in the name, written by versions before 0.2 '
                    'when an area is selected: rename them with the actual years '
                    '(e.g. _40-40_ -> _1940-2022_) and run again: ' + ', '.join(legacy))
        raise FileNotFoundError(msg)
    _, last = year_range(files, dataset, var, freq, grid, levelout)
    return int(last) + 1, datetime.datetime.now().year - 1


def align_time(filename):
    """
    Set a common time axis for monthly means: shift the time axis so that the
    first step is at 00:00 (accumulated fields are stamped at 06:00).
    Useful for loading several variables together with xarray.
    """
    cdo = get_cdo()
    first_time = cdo.showtime(input=f'-seltimestep,1 {filename}', options='-s')[0].strip()
    hours, minutes, _ = (int(x) for x in first_time.split(':'))
    if hours == 0 and minutes == 0:
        return False
    log.info('Aligning %s: shifting time axis by -%02d:%02d', filename, hours, minutes)
    filename = Path(filename)
    tmpfile = filename.with_name(filename.name + '.part')
    cdo.shifttime(f'-{hours * 60 + minutes}minutes', input=str(filename), output=str(tmpfile),
                  options=NC_OPTIONS)
    tmpfile.replace(filename)
    return True


def merge_monthly(destdir, dataset, var, freq, grid, levelout, area, update=False, align=False):
    """
    Merge yearly monthly-mean files into a single year1-year2 file and remove the yearly ones.
    In update mode the existing merged file is extended.
    """
    yearly = find_files(destdir, dataset, var, freq, grid, levelout, area)
    if not yearly:
        log.info('No yearly files to merge in %s', destdir)
        return None

    if align:
        for f in yearly:
            align_time(f)

    merged = find_files(destdir, dataset, var, freq, grid, levelout, area, merged=True) if update else []
    if merged:
        _, last_merged = year_range(merged, dataset, var, freq, grid, levelout)
        first_new, _ = year_range(yearly, dataset, var, freq, grid, levelout)
        if first_new <= last_merged:
            raise ValueError(f'Cannot extend {merged}: new data start in {first_new}')

    inputs = merged + yearly
    first, last = year_range(inputs, dataset, var, freq, grid, levelout)
    mergefile = Path(destdir, create_filename(dataset, var, freq, grid, levelout, area, first, last) + '.nc')
    tmpfile = mergefile.with_name(mergefile.name + '.part')

    log.info('Merging %d files into %s', len(inputs), mergefile)
    get_cdo().cat(input=' '.join(inputs), output=str(tmpfile), options=NC_OPTIONS)
    tmpfile.replace(mergefile)
    for f in inputs:
        if Path(f) != mergefile:
            Path(f).unlink()
    return mergefile


def daily_mean(storedir, dataset, var, freq, grid, levelout, area):
    """
    Compute a single daily mean file from all the yearly sub-daily files.
    Older daily files of the same dataset (same variable, grid, level and area) whose
    years are covered by the new file are removed, so that no period is duplicated.
    """
    destdir = Path(storedir, var, freq)
    daydir = Path(storedir, var, 'day')
    daydir.mkdir(parents=True, exist_ok=True)

    files = find_files(destdir, dataset, var, freq, grid, levelout, area)
    if not files:
        log.info('No files for daily means in %s', destdir)
        return None
    first, last = year_range(files, dataset, var, freq, grid, levelout)
    dayfile = Path(daydir, create_filename(dataset, var, 'day', grid, levelout, area,
                                           f'{first}-{last}') + '.nc')
    tmpfile = dayfile.with_name(dayfile.name + '.part')

    log.info('Creating daily file %s', dayfile)
    get_cdo().daymean(input='-cat ' + ' '.join(files), output=str(tmpfile), options=NC_OPTIONS)
    tmpfile.replace(dayfile)
    remove_superseded_daily(dayfile, first, last, dataset, var, grid, levelout, area)
    return dayfile


def remove_superseded_daily(dayfile, first, last, dataset, var, grid, levelout, area):
    """
    Remove older daily files (e.g. ..._day_..._1979-2019.nc after creating ..._1979-2025.nc).
    A file is kept if it covers years outside the new file, e.g. when old yearly
    sub-daily files have been deleted from the archive.
    """
    pattern = Path(dayfile.parent, create_filename(dataset, var, 'day', grid, levelout, area, '????-????') + '.nc')
    for old in sorted(glob.glob(str(pattern))):
        old = Path(old)
        if old == dayfile:
            continue
        old_first, old_last = year_range([old], dataset, var, 'day', grid, levelout)
        if first <= old_first and old_last <= last:
            log.info('Removing superseded daily file %s', old)
            old.unlink()
        else:
            log.warning('Keeping %s: it covers years not available in %s', old, dayfile.name)

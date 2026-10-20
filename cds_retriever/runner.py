"""Library entry point: parallel retrieval and postprocessing of a list of variables.

Example:
    from cds_retriever import retrieve
    result = retrieve({'tmpdir': '/scratch/era5', 'storedir': '/data/era5',
                       'varlist': ['2m_temperature', 'total_precipitation'],
                       'year': {'begin': 2024, 'end': 2025}, 'merge': False})
    result.files  # {'2m_temperature': [Path(...2024.nc), Path(...2025.nc)], ...}
"""

import datetime
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from .api import estimate_cost, last_available, validate_request
from .config import load_config, validate_config
from .download import (choose_request_mode, concat_months, group_retrieve, is_year_complete,
                       month_retrieve, year_retrieve)
from .postproc import align_time, daily_mean, merge_monthly, which_new_years_download, year_convert
from .request import PARAM_IDS, build_request, create_filename, month_chunks

log = logging.getLogger('cds_retriever')
LOG_FORMAT = '%(asctime)s %(levelname)s %(processName)s: %(message)s'
MONTHS = [str(i).zfill(2) for i in range(1, 13)]


class RetrievalError(RuntimeError):
    """Some downloads or postprocessing steps failed. `failures` maps job labels to exceptions,
    `result` holds what was produced anyway."""

    def __init__(self, failures, result=None):
        self.failures = failures
        self.result = result
        super().__init__(f'{len(failures)} jobs failed: ' + ', '.join(failures))


@dataclass
class Request:
    """A planned CDS request: one per variable, or one for a group of variables"""
    variables: List[str]
    dataset: str          # CDS dataset identifier
    years: List[str]
    mode: str             # 'yearly' or 'monthly' chunks
    request: dict         # the first request (first year, and first month for monthly chunks)
    cost: Optional[float] = None
    limit: Optional[float] = None


@dataclass
class RetrieveResult:
    """Outcome of retrieve()"""
    config: dict
    requests: List[Request] = field(default_factory=list)
    files: Dict[str, List[Path]] = field(default_factory=dict)  # variable -> netCDF files produced


def _worker_logging(level):
    """Logging in the worker processes: they do not inherit the configuration with spawn"""
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(format=LOG_FORMAT)
    root.setLevel(level)


def run_parallel(func, jobs, nprocs):
    """
    Run func(**kwargs) for each (label, kwargs) job in a process pool.
    Exceptions raised in the workers are collected, not lost.

    Returns:
        dict: label -> exception for the failed jobs
    """
    failures = {}
    if not jobs:
        return failures
    level = logging.getLogger().getEffectiveLevel()
    with ProcessPoolExecutor(max_workers=nprocs, initializer=_worker_logging, initargs=(level,)) as executor:
        futures = {executor.submit(func, **kwargs): label for label, kwargs in jobs}
        for future in as_completed(futures):
            label = futures[future]
            try:
                future.result()
                log.info('%s: done', label)
            except Exception as exc:
                log.error('%s: FAILED: %s', label, exc)
                failures[label] = exc
    return failures


def _request_kwargs(config):
    return dict(grid=config['grid'], levelout=config['levelout'], area=config['area'],
                product_type=config['product_type'])


def check_request(config, variables, years):
    """
    Validate the request for one or more variables against the CDS catalogue.

    Returns:
        tuple: (CDS dataset identifier, request for the first year)
    """
    kwargs = _request_kwargs(config)
    name, request = build_request(config['dataset'], variables, config['freq'], years, MONTHS, **kwargs)
    if config['check_request']:
        try:
            errors = validate_request(name, request)
        except Exception as exc:
            log.warning('Cannot validate request against the CDS catalogue: %s', exc)
            errors = []
        if errors:
            raise ValueError(f'Invalid request for {", ".join(variables)}:\n  - ' + '\n  - '.join(errors))
    _, yearly = build_request(config['dataset'], variables, config['freq'], years[0], MONTHS, **kwargs)
    return name, yearly


def check_variable(config, var, years):
    """
    Validate the request for a variable and choose yearly/monthly chunks
    if download_request is 'auto'.
    """
    name, yearly = check_request(config, [var], years)
    mode = config['download_request']
    if mode == 'auto':
        mode = choose_request_mode(name, yearly)
    log.info('%s: dataset %s, %s requests', var, name, mode)
    return name, yearly, mode


def last_complete_year(config):
    """Last year fully available on the CDS (previous year if the catalogue cannot be reached)"""
    try:
        last = last_available(config['dataset'], config['freq'], config['levelout'])
    except Exception as exc:
        log.warning('Cannot get the data availability from the CDS catalogue (%s)', exc)
        return datetime.date.today().year - 1
    end_of_year = last.month == 12 and (config['freq'] == 'mon' or last.day == 31)
    return last.year if end_of_year else last.year - 1


def years_to_process(config, var, years=None, last_year=None):
    """Years of a variable to retrieve: explicit, from the configuration or the missing ones in update mode"""
    if years is not None:
        return [str(y) for y in years]
    if config['year']['update']:
        year1, _ = which_new_years_download(config['storedir'], config['dataset'], var, config['freq'],
                                            config['grid'], config['levelout'], config['area'])
        year2 = last_year if last_year is not None else last_complete_year(config)
        log.info('%s: update from %s to %s', var, year1, year2)
        return [str(y) for y in range(year1, year2 + 1)]
    return [str(y) for y in range(config['year']['begin'], config['year']['end'] + 1)]


def plan_requests(config, years_by_var):
    """
    Group the variables sharing the same years into a single request when possible
    (group_variables: auto, variables listed in PARAM_IDS, yearly request within the CDS limit),
    the other ones get their own request.

    Returns:
        list of Request (cost and limit filled for grouped requests only)
    """
    requests = []
    single = [var for var, years in years_by_var.items() if years]
    if config['group_variables'] == 'auto' and config['download_request'] != 'monthly':
        groups = {}
        for var in single:
            if var in PARAM_IDS:
                groups.setdefault(tuple(years_by_var[var]), []).append(var)
        for years, variables in groups.items():
            if len(variables) < 2:
                continue
            name, yearly = check_request(config, variables, list(years))
            try:
                cost, limit = estimate_cost(name, yearly)
            except Exception as exc:
                log.warning('Cannot estimate the cost of the grouped request (%s)', exc)
                continue
            if cost > limit:
                log.info('Grouped request too large (%d > %d), one request per variable', cost, limit)
                continue
            log.info('%s: single request to %s for %d variables (cost %d, limit %d)',
                     ', '.join(variables), name, len(variables), cost, limit)
            requests.append(Request(variables, name, list(years), 'yearly', yearly, cost, limit))
            single = [var for var in single if var not in variables]

    for var in single:
        name, yearly, mode = check_variable(config, var, years_by_var[var])
        requests.append(Request([var], name, years_by_var[var], mode, yearly))
    return requests


def retrieve_variable(config, var, years, mode, savedir):
    """
    Download all years of a variable into savedir.

    Yearly requests are one job per year. Monthly requests are one job per
    (year, month), so that all the nprocs workers are busy even with few years,
    followed by the concatenation of the chunks of each year.
    The pool size nprocs is the maximum number of simultaneous CDS requests.

    Returns:
        dict: label -> exception for the failed jobs
    """
    common = dict(dataset=config['dataset'], var=var, freq=config['freq'], grid=config['grid'],
                  levelout=config['levelout'], area=config['area'], outdir=savedir)
    options = dict(product_type=config['product_type'], retries=config['retries'])
    nprocs = config['nprocs']

    if mode == 'yearly':
        jobs = [(f'{var} {year} retrieve', dict(common, year=year, request=mode, **options))
                for year in years]
        return run_parallel(year_retrieve, jobs, nprocs)

    todo = [year for year in years if not is_year_complete(year=year, **common)]
    jobs = [(f'{var} {year}-{month} retrieve', dict(common, year=year, month=month, **options))
            for year in todo for (month,) in month_chunks(mode)]
    failures = run_parallel(month_retrieve, jobs, nprocs)

    # years with a missing month are not concatenated, complete chunks are kept for the next run
    failed_years = {kwargs['year'] for label, kwargs in jobs if label in failures}
    jobs = [(f'{var} {year} concat', dict(common, year=year))
            for year in todo if year not in failed_years]
    failures.update(run_parallel(concat_months, jobs, nprocs))
    return failures


def retrieve_group(config, variables, years):
    """
    Download all years of a group of variables, one request per year, split per variable.

    Returns:
        dict: label -> exception for the failed jobs
    """
    common = dict(dataset=config['dataset'], variables=variables, freq=config['freq'],
                  grid=config['grid'], levelout=config['levelout'], area=config['area'],
                  tmpdir=config['tmpdir'], product_type=config['product_type'],
                  retries=config['retries'])
    label = f'{variables[0]}+{len(variables) - 1}'
    jobs = [(f'{label} {year} retrieve', dict(common, year=year)) for year in years]
    return run_parallel(group_retrieve, jobs, config['nprocs'])


def postprocess_variable(config, var, years):
    """
    Convert the yearly GRIB files of a variable to netCDF, then merge monthly data
    (if merge is set) or compute daily means of sub-daily data.

    Returns:
        tuple: (list of netCDF files produced, dict of failures)
    """
    dataset, freq, grid = config['dataset'], config['freq'], config['grid']
    levelout, area = config['levelout'], config['area']
    savedir = Path(config['tmpdir'], var)
    destdir = Path(config['storedir'], var, freq)
    destdir.mkdir(parents=True, exist_ok=True)

    jobs, yearly = [], []
    for year in years:
        filename = create_filename(dataset, var, freq, grid, levelout, area, year)
        yearly.append(Path(destdir, filename + '.nc'))
        jobs.append((f'{var} {year} convert',
                     dict(infile=Path(savedir, filename + '.grib'), outfile=yearly[-1])))
    failures = run_parallel(year_convert, jobs, config['nprocs'])
    if failures:
        return [], failures

    if freq == 'mon':
        if config['do_align']:
            for f in yearly:
                align_time(f)
        if config['merge']:
            merged = merge_monthly(destdir, dataset, var, freq, grid, levelout, area,
                                   update=config['year']['update'])
            return ([merged] if merged else []), {}
        return yearly, {}
    if freq != 'instant':
        dayfile = daily_mean(config['storedir'], dataset, var, freq, grid, levelout, area)
        return yearly + ([dayfile] if dayfile else []), {}
    return yearly, {}


def retrieve(config, *, variables=None, years=None, dry_run=False):
    """
    Retrieve and postprocess ERA5 / ERA5-Land data.

    Parameters:
        config (dict, str or Path): configuration, or path of the YAML configuration file.
            Missing keys take the defaults of config.DEFAULTS.
        variables (list of str): variables to process instead of config['varlist'].
        years (iterable of int): years to retrieve, instead of year.begin/end or year.update.
        dry_run (bool): validate the requests and estimate their cost, without downloading.

    Returns:
        RetrieveResult: the planned requests and the netCDF files produced for each variable.

    Raises:
        ValueError: invalid configuration or request.
        RetrievalError: some jobs failed (the files produced anyway are in its result).
    """
    config = load_config(config, validate=False)
    if variables is not None:
        config['varlist'] = [variables] if isinstance(variables, str) else list(variables)
    if years is not None:
        years = sorted(int(y) for y in years)
        config['year'] = {'begin': years[0], 'end': years[-1], 'update': False}
    config = validate_config(config)
    result = RetrieveResult(config)
    update = years is None and config['year']['update']
    last_year = last_complete_year(config) if update else None

    years_by_var = {var: years_to_process(config, var, years, last_year) for var in config['varlist']}
    for var, var_years in years_by_var.items():
        if not var_years:
            log.info('%s: everything has already been downloaded', var)

    if config['do_retrieve']:
        result.requests = plan_requests(config, years_by_var)

    if dry_run:
        for req in result.requests:
            if req.cost is None:
                first = dict(req.request, month=['01']) if req.mode == 'monthly' else req.request
                req.cost, req.limit = estimate_cost(req.dataset, first)
            log.info('%s: %d %s requests to %s (first one: cost %d, limit %d): %s',
                     ', '.join(req.variables), len(req.years) * (12 if req.mode == 'monthly' else 1),
                     req.mode, req.dataset, req.cost, req.limit, req.request)
        return result

    failures = {}
    failed_vars = set()
    for req in result.requests:
        if len(req.variables) > 1:
            fails = retrieve_group(config, req.variables, req.years)
        else:
            var = req.variables[0]
            savedir = Path(config['tmpdir'], var)
            savedir.mkdir(parents=True, exist_ok=True)
            fails = retrieve_variable(config, var, req.years, req.mode, savedir)
        if fails:
            log.error('%s: %d downloads failed, skipping postprocessing', ', '.join(req.variables), len(fails))
            failures.update(fails)
            failed_vars.update(req.variables)

    if config['do_postproc']:
        for var, var_years in years_by_var.items():
            if var in failed_vars:
                continue
            if not var_years and config['freq'] == 'mon':
                continue  # monthly data are already merged, while daily means are recomputed
            try:
                files, fails = postprocess_variable(config, var, var_years)
            except Exception as exc:
                files, fails = [], {f'{var} postproc': exc}
            result.files[var] = files
            failures.update(fails)

    if failures:
        raise RetrievalError(failures, result)
    return result


"""Command-line entry point: parallel retrieval and postprocessing of ERA5 data"""

import logging
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from .api import estimate_cost, validate_request
from .config import apply_args, load_config, parser, print_config, validate_config
from .download import choose_request_mode, year_retrieve
from .postproc import daily_mean, merge_monthly, which_new_years_download, year_convert
from .request import build_request, create_filename

log = logging.getLogger('cds_retriever')
LOG_FORMAT = '%(asctime)s %(levelname)s %(processName)s: %(message)s'


def setup_logging(level='INFO'):
    """Configure logging (also called in the worker processes, which do not inherit it with spawn)"""
    logging.basicConfig(level=level.upper(), format=LOG_FORMAT, force=True)


def run_parallel(func, jobs, nprocs):
    """
    Run func(**kwargs) for each (label, kwargs) job in a process pool.
    Exceptions raised in the workers are collected, not lost.

    Returns:
        dict: label -> exception for the failed jobs
    """
    failures = {}
    level = logging.getLevelName(logging.getLogger().getEffectiveLevel())
    with ProcessPoolExecutor(max_workers=nprocs, initializer=setup_logging, initargs=(level,)) as executor:
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


def check_variable(config, var, years):
    """
    Validate the request for a variable against the CDS catalogue and choose
    yearly/monthly chunks if download_request is 'auto'.
    """
    kwargs = dict(grid=config['grid'], levelout=config['levelout'], area=config['area'],
                  product_type=config['product_type'])
    months = [str(i).zfill(2) for i in range(1, 13)]
    name, request = build_request(config['dataset'], var, config['freq'], years, months, **kwargs)

    if config['check_request']:
        try:
            errors = validate_request(name, request)
        except Exception as exc:
            log.warning('Cannot validate request against the CDS catalogue: %s', exc)
            errors = []
        if errors:
            raise ValueError(f'Invalid request for {var}:\n  - ' + '\n  - '.join(errors))

    mode = config['download_request']
    _, yearly = build_request(config['dataset'], var, config['freq'], years[0], months, **kwargs)
    if mode == 'auto':
        mode = choose_request_mode(name, yearly)
    log.info('%s: dataset %s, %s requests', var, name, mode)
    return name, yearly, mode


def process_variable(config, var, dry_run=False):
    """Retrieve and postprocess a single variable. Returns the dict of failures."""
    dataset, freq, grid = config['dataset'], config['freq'], config['grid']
    levelout, area = config['levelout'], config['area']
    tmpdir, storedir, nprocs = config['tmpdir'], config['storedir'], config['nprocs']
    update = config['year']['update']
    do_retrieve, do_postproc = config['do_retrieve'], config['do_postproc']

    if update:
        year1, year2 = which_new_years_download(storedir, dataset, var, freq, grid, levelout, area)
        log.info('%s: update from %s to %s', var, year1, year2)
        if year1 > year2:
            log.info('%s: everything has already been downloaded', var)
            do_retrieve = False
            # monthly data are already merged, while daily means are recomputed
            do_postproc = do_postproc and freq != 'mon'
    else:
        year1, year2 = config['year']['begin'], config['year']['end']
    years = [str(y) for y in range(year1, year2 + 1)]

    if do_retrieve:
        name, request, mode = check_variable(config, var, years)
        if dry_run:
            if mode == 'monthly':
                request = dict(request, month=['01'])
            cost, limit = estimate_cost(name, request)
            log.info('%s: first %s request to %s (cost %d, limit %d): %s',
                     var, mode, name, cost, limit, request)
            return {}
    elif dry_run:
        return {}

    savedir = Path(tmpdir, var)
    savedir.mkdir(parents=True, exist_ok=True)

    if do_retrieve:
        jobs = [(f'{var} {year} retrieve',
                 dict(dataset=dataset, var=var, freq=freq, year=year, grid=grid, levelout=levelout,
                      area=area, outdir=savedir, request=mode,
                      product_type=config['product_type'], retries=config['retries']))
                for year in years]
        failures = run_parallel(year_retrieve, jobs, nprocs)
        if failures:
            log.error('%s: %d years failed to download, skipping postprocessing', var, len(failures))
            return failures

    if not do_postproc:
        return {}

    destdir = Path(storedir, var, freq)
    destdir.mkdir(parents=True, exist_ok=True)
    jobs = []
    for year in years:
        filename = create_filename(dataset, var, freq, grid, levelout, area, year)
        jobs.append((f'{var} {year} convert',
                     dict(infile=Path(savedir, filename + '.grib'), outfile=Path(destdir, filename + '.nc'))))
    failures = run_parallel(year_convert, jobs, nprocs)
    if failures:
        return failures

    if freq == 'mon':
        merge_monthly(destdir, dataset, var, freq, grid, levelout, area,
                      update=update, align=config['do_align'])
    elif freq != 'instant':
        daily_mean(storedir, dataset, var, freq, grid, levelout, area)
    return {}


def main(argv=None):
    """Main entry point"""
    args = parser(argv)
    setup_logging(args.loglevel)

    try:
        config = validate_config(apply_args(load_config(args.config), args))
    except (OSError, ValueError) as exc:
        log.error('%s', exc)
        return 1
    print_config(config)

    failures = {}
    for var in config['varlist']:
        try:
            failures.update(process_variable(config, var, dry_run=args.dry_run))
        except Exception as exc:
            log.error('%s: %s', var, exc)
            failures[var] = exc

    if failures:
        log.error('Completed with %d failures: %s', len(failures), ', '.join(failures))
        return 1
    log.info('All done!')
    return 0


if __name__ == '__main__':
    sys.exit(main())

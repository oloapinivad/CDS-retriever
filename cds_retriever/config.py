"""Configuration: command-line parser, YAML loading, defaults and validation"""

import argparse
import copy
import logging
from collections.abc import Mapping

import yaml

from .request import DATASETS, FREQS, GRIDS, PRODUCT_TYPES, REQUEST_MODES, dataset_id, define_level, is_daily

log = logging.getLogger(__name__)

DEFAULTS = {
    'dataset': 'ERA5',
    'product_type': 'reanalysis',
    'year': {'begin': None, 'end': None, 'update': False},
    'freq': 'mon',
    'levelout': 'sfc',
    'grid': 'full',
    'area': 'global',
    'nprocs': 4,
    'download_request': 'auto',
    'retries': 3,
    'check_request': True,
    'do_retrieve': True,
    'do_postproc': True,
    'do_align': False,
    'merge': True,
    'group_variables': 'auto',
}
GROUP_MODES = ('auto', False)
REQUIRED = ('tmpdir', 'storedir', 'varlist')


def parser(argv=None):
    """
    Command-line parser
    """
    internal_parser = argparse.ArgumentParser(
        description="Parallel retrieval and postprocessing of ERA5 data from the Climate Data Store")

    internal_parser.add_argument("-c", "--config", required=True, help="Path to the YAML configuration file")
    internal_parser.add_argument("-n", "--nprocs", type=int, help="Number of parallel processes")
    internal_parser.add_argument("-u", "--update", action="store_true", help="Update existing dataset")
    internal_parser.add_argument("-v", "--variable", type=str, help="Pick a specific variable")
    internal_parser.add_argument("-l", "--levelout", type=str, help="Pick a specific level")
    internal_parser.add_argument("--outputdir", type=str, help="Output (store) directory")
    internal_parser.add_argument("--tmpdir", type=str, help="Temporary (download) directory")
    internal_parser.add_argument("--dry-run", action="store_true",
                                 help="Validate the requests and print them with their cost, without downloading")
    internal_parser.add_argument("--loglevel", default="INFO", help="Logging level (DEBUG, INFO, WARNING)")

    return internal_parser.parse_args(argv)


def _merge(defaults, config):
    merged = copy.deepcopy(defaults)
    for key, value in config.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(source, validate=True):
    """
    Build a configuration from a YAML file or a dictionary, filling in the defaults.

    Parameters:
        source (str, Path or dict): path of the YAML file, or the configuration itself
            (not modified, a copy is returned).
        validate (bool): validate the configuration (see validate_config).

    Returns:
        dict: the configuration
    """
    if isinstance(source, Mapping):
        config = dict(source)
    else:
        with open(source, 'r', encoding='utf8') as file:
            config = yaml.safe_load(file) or {}
        if not isinstance(config, dict):
            raise ValueError(f'{source} is not a valid configuration file')
    config = _merge(DEFAULTS, config)
    return validate_config(config) if validate else config


def apply_args(config, args):
    """Override the configuration with command-line arguments"""
    config = copy.deepcopy(config)
    overrides = {
        'nprocs': args.nprocs,
        'varlist': [args.variable] if args.variable else None,
        'levelout': args.levelout,
        'storedir': args.outputdir,
        'tmpdir': args.tmpdir,
    }
    for key, value in overrides.items():
        if value is not None:
            log.info('Overriding %s (%s) with command-line value (%s)', key, config.get(key), value)
            config[key] = value
    if args.update:
        config['year']['update'] = True
    return config


def validate_config(config):
    """
    Check the configuration, raising ValueError with all the problems found.
    Normalises varlist to a list and years to int.
    """
    errors = []
    for key in REQUIRED:
        if not config.get(key):
            errors.append(f'{key} is required')

    if isinstance(config.get('varlist'), str):
        config['varlist'] = [config['varlist']]

    if config['group_variables'] is True:
        config['group_variables'] = 'auto'
    choices = {'dataset': DATASETS, 'freq': FREQS, 'grid': GRIDS, 'product_type': PRODUCT_TYPES,
               'download_request': REQUEST_MODES, 'group_variables': GROUP_MODES}
    for key, allowed in choices.items():
        if config[key] not in allowed:
            errors.append(f'{key}: {config[key]} is not valid, options are {allowed}')

    try:
        _, level_kind = define_level(config['levelout'])
    except ValueError as exc:
        errors.append(f'levelout: {exc}')
    else:
        if config['dataset'] in DATASETS and config['freq'] in FREQS:
            try:
                dataset_id(config['dataset'], level_kind, config['freq'])
            except ValueError as exc:
                errors.append(str(exc))

    if is_daily(config['freq']) and config['grid'] != 'full':
        errors.append('grid selection is not available for daily statistics: use grid: full')

    area = config['area']
    if area != 'global':
        if not (isinstance(area, (list, tuple)) and len(area) == 4):
            errors.append("area must be 'global' or [North, West, South, East]")
        elif area[0] < area[2]:
            errors.append('area: North must be larger than South')

    year = config['year']
    if not year.get('update'):
        if year.get('begin') is None or year.get('end') is None:
            errors.append('year.begin and year.end are required unless year.update is set')
        else:
            year['begin'], year['end'] = int(year['begin']), int(year['end'])
            if year['begin'] > year['end']:
                errors.append('year.begin must not be after year.end')

    if not isinstance(config['nprocs'], int) or config['nprocs'] < 1:
        errors.append('nprocs must be a positive integer')

    if errors:
        raise ValueError('Invalid configuration:\n  - ' + '\n  - '.join(errors))
    return config


def print_config(conf_dict):
    """
    Print the configuration options
    """
    year = conf_dict['year']
    lines = [
        f"Downloading files in {conf_dict['tmpdir']}",
        f"Storing final files in {conf_dict['storedir']}",
        f"Downloading {conf_dict['varlist']} from {conf_dict['dataset']} ({conf_dict['product_type']})",
        'Updating existing datasets' if year['update'] else f"Data range: {year['begin']}-{year['end']}",
        f"Vertical levels: {conf_dict['levelout']}",
        f"Data frequency: {conf_dict['freq']}",
        f"Grid selection: {conf_dict['grid']}",
        f"Area: {conf_dict['area']}",
        f"Number of parallel processes: {conf_dict['nprocs']}",
        f"Download request chunks: {conf_dict['download_request']}",
        'Actions: ' + ', '.join(name for name, flag in [
            ('retrieve', conf_dict['do_retrieve']),
            ('postproc', conf_dict['do_postproc']),
            ('align monthly time axis', conf_dict['do_align']),
            ('merge monthly files', conf_dict['merge'] and conf_dict['freq'] == 'mon')] if flag),
        f"Group variables in a single request: {conf_dict['group_variables']}",
    ]
    for line in lines:
        log.info(line)

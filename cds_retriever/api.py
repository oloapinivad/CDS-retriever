"""Query the public CDS catalogue: request validation, cost estimation and data availability.

The CDS (2024+ infrastructure, OGC API Processes) exposes, without authentication:
  - GET  <url>/retrieve/v1/processes/<dataset>          inputs schema (allowed keys and values)
  - POST <url>/retrieve/v1/processes/<dataset>/costing  request size and limit
  - GET  <url>/catalogue/v1/collections/<dataset>       metadata, including the time extent
    and the link to the constraints (the valid combinations of the request keys)
"""

import datetime
import json
import logging
import os
import urllib.request
from functools import lru_cache
from pathlib import Path

from .request import dataset_id, define_level

DEFAULT_URL = 'https://cds.climate.copernicus.eu/api'

# keys accepted by the backend even if not listed in the web form schema
EXTRA_KEYS = ('grid',)

log = logging.getLogger(__name__)


def api_url():
    """CDS API url from CDSAPI_URL, the cdsapirc file or the default one"""
    if os.environ.get('CDSAPI_URL'):
        return os.environ['CDSAPI_URL'].rstrip('/')
    rcfile = Path(os.environ.get('CDSAPI_RC', Path.home() / '.cdsapirc'))
    if rcfile.exists():
        for line in rcfile.read_text(encoding='utf8').splitlines():
            key, _, value = line.partition(':')
            if key.strip() == 'url' and value.strip():
                return value.strip().rstrip('/')
    return DEFAULT_URL


def _call(url, payload=None, timeout=30):
    data = None
    headers = {'Accept': 'application/json'}
    if payload is not None:
        data = json.dumps(payload).encode()
        headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.load(response)


@lru_cache(maxsize=None)
def get_process(dataset, url=None):
    """Return the process description (including the inputs schema) of a dataset"""
    url = url or api_url()
    return _call(f'{url}/retrieve/v1/processes/{dataset}')


def validate_request(dataset, request, process=None):
    """
    Check a request against the inputs schema published by the CDS.

    Returns:
        list of str: problems found, empty if the request looks valid.
    """
    process = process or get_process(dataset)
    inputs = process.get('inputs', {})
    errors = []
    for key, value in request.items():
        if key not in inputs:
            if key not in EXTRA_KEYS:
                errors.append(f"'{key}' is not a valid key for {dataset}")
            continue
        schema = inputs[key].get('schema', {})
        allowed = schema.get('enum') or schema.get('items', {}).get('enum')
        if not allowed:
            continue
        values = value if isinstance(value, (list, tuple)) else [value]
        bad = [v for v in values if str(v) not in allowed]
        if bad:
            errors.append(f"{key}: {bad} not available for {dataset}")
    return errors


def estimate_cost(dataset, request, url=None):
    """
    Return (cost, limit) of a request as computed by the CDS.
    For ERA5 the cost is the number of fields (variables x levels x time steps).
    """
    url = url or api_url()
    # keys outside the schema are counted as extra dimensions (e.g. grid doubles the cost)
    request = {k: v for k, v in request.items() if k not in EXTRA_KEYS}
    result = _call(f'{url}/retrieve/v1/processes/{dataset}/costing', {'inputs': request})
    return float(result['cost']), float(result['limit'])


def get_collection(dataset, url=None):
    """Return the catalogue entry of a dataset (not cached: it changes with every data release)"""
    url = url or api_url()
    return _call(f'{url}/catalogue/v1/collections/{dataset}')


def available_until(name, url=None):
    """
    Date of the last time step published for a CDS dataset identifier
    (e.g. 'reanalysis-era5-single-levels-monthly-means'), from the catalogue time extent.

    Returns:
        datetime.date
    """
    intervals = get_collection(name, url=url)['extent']['temporal']['interval']
    ends = [interval[1] for interval in intervals if interval[1]]
    if not ends:
        raise ValueError(f'No end date in the time extent of {name}')
    return max(datetime.date.fromisoformat(end[:10]) for end in ends)


def get_constraints(dataset, url=None):
    """Valid combinations of the request keys of a dataset (list of {key: [values]})"""
    links = get_collection(dataset, url=url).get('links', [])
    href = next((link['href'] for link in links if link.get('rel') == 'constraints'), None)
    if href is None:
        raise ValueError(f'No constraints published for {dataset}')
    return _call(href)


def last_available_in(dataset, url=None, **selection):
    """
    Last date available in any CDS dataset for a selection of the request keys,
    e.g. last_available_in('satellite-sea-surface-temperature', processinglevel='level_4',
    temporal_resolution='monthly'): from the constraints, more precise than the
    time extent of the catalogue, which covers all the products of the dataset.

    Returns:
        datetime.date: the first day of the last month, or the last day when
            the constraints select days.
    """
    last = None
    for combination in get_constraints(dataset, url=url):
        if not all(str(value) in combination.get(key, []) for key, value in selection.items()):
            continue
        if not combination.get('year') or not combination.get('month'):
            continue
        # each combination is the cartesian product of its values
        date = datetime.date(max(int(y) for y in combination['year']),
                             max(int(m) for m in combination['month']),
                             max((int(d) for d in combination.get('day', [])), default=1))
        last = date if last is None else max(last, date)
    if last is None:
        raise ValueError(f'No data in {dataset} for {selection}')
    return last


def last_available(dataset='ERA5', freq='mon', levelout='sfc', url=None):
    """
    Last date available on the CDS for an ERA5 / ERA5-Land configuration.
    For monthly means it is the first day of the last month available.

    Returns:
        datetime.date
    """
    _, level_kind = define_level(levelout)
    return available_until(dataset_id(dataset, level_kind, freq), url=url)

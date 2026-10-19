"""Query the public CDS catalogue: request validation and cost estimation.

The CDS (2024+ infrastructure, OGC API Processes) exposes, without authentication:
  - GET  <url>/retrieve/v1/processes/<dataset>          inputs schema (allowed keys and values)
  - POST <url>/retrieve/v1/processes/<dataset>/costing  request size and limit
"""

import json
import logging
import os
import urllib.request
from functools import lru_cache
from pathlib import Path

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

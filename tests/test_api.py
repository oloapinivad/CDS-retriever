import pytest

from cds_retriever.api import estimate_cost, validate_request
from cds_retriever.download import choose_request_mode
from cds_retriever.request import build_request

MONTHS = [str(i).zfill(2) for i in range(1, 13)]

PROCESS = {'inputs': {
    'variable': {'schema': {'type': 'array', 'items': {'enum': ['2m_temperature']}}},
    'year': {'schema': {'type': 'string', 'enum': ['2000', '2001']}},
    'data_format': {'schema': {'type': 'string', 'enum': ['grib', 'netcdf']}},
}}


def test_validate_request_offline():
    assert validate_request('x', {'variable': ['2m_temperature'], 'year': '2000', 'grid': ['1', '1']},
                            process=PROCESS) == []
    errors = validate_request('x', {'variable': ['foo'], 'year': ['1999'], 'bogus': 1}, process=PROCESS)
    assert len(errors) == 3


@pytest.mark.network
@pytest.mark.parametrize('dataset, var, freq, levelout', [
    ('ERA5', '2m_temperature', 'mon', 'sfc'),
    ('ERA5', 'total_precipitation', '1hr', 'sfc'),
    ('ERA5', 'temperature', '6hrs', 'plev19'),
    ('ERA5', 'geopotential', 'mon', '500hPa'),
    ('ERA5-Land', '2m_temperature', '1hr', 'sfc'),
    ('ERA5-Land', 'total_precipitation', 'mon', 'sfc'),
])
def test_requests_match_cds_catalogue(dataset, var, freq, levelout):
    """The requests we build are accepted by the current CDS schema"""
    name, request = build_request(dataset, var, freq, '2000', MONTHS, grid='2.5x2.5', levelout=levelout,
                                  area=[65, -15, 25, 45])
    assert validate_request(name, request) == []


@pytest.mark.network
def test_choose_request_mode():
    name, request = build_request('ERA5', 'total_precipitation', '1hr', '2000', MONTHS)
    assert choose_request_mode(name, request) == 'yearly'
    name, request = build_request('ERA5', 'temperature', '1hr', '2000', MONTHS, levelout='plev37')
    cost, limit = estimate_cost(name, request)
    assert cost > limit
    assert choose_request_mode(name, request) == 'monthly'


@pytest.mark.network
def test_cost_ignores_grid():
    name, request = build_request('ERA5', '2m_temperature', 'mon', '2000', MONTHS, grid='2.5x2.5')
    assert estimate_cost(name, request)[0] == 12

import datetime
import logging
import shutil
import subprocess
from pathlib import Path

import pytest

from cds_retriever import RetrievalError, api, load_config, retrieve, runner
from cds_retriever import download as dl


def serial_run(func, jobs, nprocs):
    """In-process replacement of run_parallel, so that monkeypatching works"""
    failures = {}
    for label, kwargs in jobs:
        try:
            func(**kwargs)
        except Exception as exc:
            failures[label] = exc
    return failures


@pytest.fixture
def base(tmp_path):
    return {'tmpdir': str(tmp_path / 'tmp'), 'storedir': str(tmp_path / 'store'),
            'varlist': ['2m_temperature', 'total_precipitation', 'not_in_table'],
            'year': {'begin': 2000, 'end': 2001}, 'merge': False}


@pytest.fixture
def backend(monkeypatch):
    """Fake CDS and CDO: downloads write the request as text, conversion copies the file"""
    calls = {'requests': [], 'costs': []}

    def fake_download(name, request, target, retries=3, client=None):
        calls['requests'].append((name, request))
        Path(target).write_text(','.join(request['variable']))

    def fake_split(infile, targets):
        for param, target in targets.items():
            Path(target).write_text(str(param))
        return list(targets.values())

    def fake_cost(name, request):
        calls['costs'].append(request['variable'])
        return 12.0 * len(request['variable']), 20000.0

    monkeypatch.setattr(runner, 'run_parallel', serial_run)
    monkeypatch.setattr(runner, 'validate_request', lambda name, request: [])
    monkeypatch.setattr(runner, 'estimate_cost', fake_cost)
    monkeypatch.setattr(dl, 'estimate_cost', fake_cost)
    monkeypatch.setattr(dl, 'get_client', lambda: None)
    monkeypatch.setattr(dl, 'download', fake_download)
    monkeypatch.setattr(dl, 'is_file_complete', lambda f, n: Path(f).exists())
    monkeypatch.setattr(dl, 'split_by_param', fake_split)
    monkeypatch.setattr(runner, 'year_convert',
                        lambda infile, outfile: shutil.copy(infile, outfile) and Path(outfile))
    return calls


def test_load_config_from_dict(base):
    config = load_config(base)
    assert config['grid'] == 'full' and config['group_variables'] == 'auto'
    assert config['year'] == {'begin': 2000, 'end': 2001, 'update': False}
    assert 'grid' not in base  # the input is not modified
    with pytest.raises(ValueError, match='group_variables'):
        load_config(dict(base, group_variables='always'))


def test_dry_run_groups_variables(base, backend):
    result = retrieve(base, dry_run=True)
    assert backend['requests'] == []
    grouped, single = result.requests
    assert grouped.variables == ['2m_temperature', 'total_precipitation'] and grouped.mode == 'yearly'
    assert grouped.cost == 24 and grouped.request['variable'] == grouped.variables
    assert single.variables == ['not_in_table'] and single.cost == 12
    assert result.files == {}


def test_retrieve_grouped(base, backend, tmp_path):
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    result = retrieve(base)
    # one request per year for the two grouped variables, one per year for the other one
    variables = [tuple(r['variable']) for _, r in backend['requests']]
    assert variables == [('2m_temperature', 'total_precipitation')] * 2 + [('not_in_table',)] * 2
    assert set(result.files) == set(base['varlist'])
    t2m = result.files['2m_temperature']
    assert [f.name for f in t2m] == [f'ERA5_2m_temperature_mon_full_sfc_{y}.nc' for y in (2000, 2001)]
    assert t2m[0].read_text() == '167'
    assert t2m[0].parent == tmp_path / 'store' / '2m_temperature' / 'mon'
    # grouped files are removed after splitting
    assert list((tmp_path / 'tmp' / '_groups').iterdir()) == []
    # the library does not configure logging
    assert root.handlers == handlers and root.level == level


def test_retrieve_no_grouping(base, backend):
    del base['year']  # not needed with explicit years
    result = retrieve(dict(base, group_variables=False), variables=['2m_temperature', 'total_precipitation'],
                      years=[2005])
    assert result.config['year'] == {'begin': 2005, 'end': 2005, 'update': False}
    assert [r['variable'] for _, r in backend['requests']] == [['2m_temperature'], ['total_precipitation']]
    assert all(r['year'] == ['2005'] for _, r in backend['requests'])


def test_grouped_split_fallback(base, backend, monkeypatch):
    def bad_split(infile, targets):
        raise dl.SplitError('unexpected paramIds')

    monkeypatch.setattr(dl, 'split_by_param', bad_split)
    result = retrieve(base, variables=['2m_temperature', 'total_precipitation'], years=[2000])
    variables = [r['variable'] for _, r in backend['requests']]
    assert variables == [['2m_temperature', 'total_precipitation'], ['2m_temperature'], ['total_precipitation']]
    assert result.files['total_precipitation'][0].read_text() == 'total_precipitation'


def test_retrieval_error(base, backend, monkeypatch):
    original = dl.download

    def flaky(name, request, target, retries=3, client=None):
        if request['variable'] == ['not_in_table']:
            raise RuntimeError('CDS error')
        original(name, request, target, retries=retries, client=client)

    monkeypatch.setattr(dl, 'download', flaky)
    with pytest.raises(RetrievalError) as err:
        retrieve(base)
    assert sorted(err.value.failures) == ['not_in_table 2000 retrieve', 'not_in_table 2001 retrieve']
    # the other variables are still processed
    assert set(err.value.result.files) == {'2m_temperature', 'total_precipitation'}


def test_update_uses_cds_availability(base, backend, monkeypatch, tmp_path):
    destdir = tmp_path / 'store' / '2m_temperature' / 'mon'
    destdir.mkdir(parents=True)
    (destdir / 'ERA5_2m_temperature_mon_full_sfc_1940-2023.nc').write_text('old')
    monkeypatch.setattr(runner, 'last_available', lambda *args: datetime.date(2025, 9, 1))
    config = dict(base, varlist=['2m_temperature'], year={'update': True})
    assert runner.last_complete_year(load_config(config)) == 2024
    result = retrieve(config, dry_run=True)
    assert result.requests[0].years == ['2024']


@pytest.mark.parametrize('last, freq, expected', [
    (datetime.date(2025, 12, 1), 'mon', 2025),
    (datetime.date(2025, 11, 1), 'mon', 2024),
    (datetime.date(2025, 12, 31), '1hr', 2025),
    (datetime.date(2025, 12, 30), '1hr', 2024),
])
def test_last_complete_year(base, monkeypatch, last, freq, expected):
    monkeypatch.setattr(runner, 'last_available', lambda *args: last)
    assert runner.last_complete_year(load_config(dict(base, freq=freq))) == expected


def test_available_until(monkeypatch):
    collection = {'extent': {'temporal': {'interval': [['1940-01-01T00:00:00+00:00',
                                                         '2026-09-01T00:00:00+00:00']]}}}
    monkeypatch.setattr(api, 'get_collection', lambda name, url=None: collection)
    assert api.last_available('ERA5', 'mon', 'plev8') == datetime.date(2026, 9, 1)


@pytest.mark.skipif(not (shutil.which('cdo') and shutil.which('grib_copy')), reason='needs cdo and ecCodes')
def test_split_by_param(tmp_path):
    files = []
    for code in (167, 228):
        out = tmp_path / f'{code}.grib'
        subprocess.run(['cdo', '-s', '-f', 'grb', f'-setparam,{code}.128', '-settaxis,2000-01-01,00:00,1mon',
                        '-duplicate,12', '-const,1,r10x5', str(out)], check=True)
        files.append(str(out))
    grouped = tmp_path / 'group.grib'
    subprocess.run(['cdo', '-s', 'merge', *files, str(grouped)], check=True)

    targets = {167: tmp_path / 't2m.grib', 228: tmp_path / 'tp.grib'}
    dl.split_by_param(grouped, targets)
    for code, target in targets.items():
        params = subprocess.run(['grib_get', '-p', 'paramId', str(target)], check=True,
                                capture_output=True, text=True).stdout.split()
        assert params == [str(code)] * 12
    assert not (tmp_path / 'group_split').exists()

    with pytest.raises(dl.SplitError, match='found \\[167, 228\\]'):
        dl.split_by_param(grouped, {167: tmp_path / 'c.grib'})
    assert not (tmp_path / 'c.grib').exists()


@pytest.mark.network
def test_last_available_live():
    last = api.last_available('ERA5', 'mon', 'sfc')
    assert datetime.date(2025, 1, 1) < last <= datetime.date.today()


def test_last_available_in(monkeypatch):
    constraints = [
        {'processinglevel': ['level_4'], 'temporal_resolution': ['monthly'], 'year': ['2025', '2026'],
         'month': ['01', '09']},
        {'processinglevel': ['level_4'], 'temporal_resolution': ['monthly'], 'year': ['2025'],
         'month': ['10', '12']},
        {'processinglevel': ['level_4'], 'temporal_resolution': ['daily'], 'year': ['2026'],
         'month': ['10'], 'day': ['01', '04']},
        {'processinglevel': ['level_3c'], 'temporal_resolution': ['monthly'], 'year': ['2027'], 'month': ['01']},
    ]
    monkeypatch.setattr(api, 'get_constraints', lambda dataset, url=None: constraints)
    assert api.last_available_in('sst', processinglevel='level_4', temporal_resolution='monthly') \
        == datetime.date(2026, 9, 1)
    assert api.last_available_in('sst', processinglevel='level_4', temporal_resolution='daily') \
        == datetime.date(2026, 10, 4)
    with pytest.raises(ValueError, match='No data'):
        api.last_available_in('sst', processinglevel='level_2')


@pytest.mark.network
def test_last_available_in_live():
    last = api.last_available_in('satellite-sea-surface-temperature', processinglevel='level_4',
                                 sensor_on_satellite='combined_product', version='3_0',
                                 temporal_resolution='monthly')
    assert datetime.date(2025, 1, 1) < last <= datetime.date.today()

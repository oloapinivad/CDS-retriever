from pathlib import Path

import pytest

from cds_retriever import runner
from cds_retriever import download as dl


class FakeCdo:
    def cat(self, input, output):
        Path(output).write_text(input)


@pytest.fixture
def fake_backend(monkeypatch):
    requests = []

    def fake_download(name, request, target, retries=3, client=None):
        requests.append((name, request))
        Path(target).write_text('grib')

    monkeypatch.setattr(dl, 'get_client', lambda: None)
    monkeypatch.setattr(dl, 'download', fake_download)
    monkeypatch.setattr(dl, 'is_file_complete', lambda f, n: Path(f).exists())
    monkeypatch.setattr(dl, 'get_cdo', FakeCdo)
    return requests


def test_year_retrieve_monthly(tmp_path, fake_backend):
    out = dl.year_retrieve('ERA5', 'temperature', '1hr', 2000, 'full', 'plev37', 'global', tmp_path,
                           request='monthly')
    assert out == tmp_path / 'ERA5_temperature_1hr_full_plev37_2000.grib'
    assert len(fake_backend) == 12
    assert fake_backend[0][1]['month'] == ['01']
    # monthly chunks are concatenated in order and removed
    assert out.read_text().split()[0].endswith('_month01.grib')
    assert sorted(p.name for p in tmp_path.iterdir()) == [out.name]


def test_year_retrieve_skips_complete(tmp_path, fake_backend):
    (tmp_path / 'ERA5_tp_mon_full_sfc_2000.grib').write_text('done')
    dl.year_retrieve('ERA5', 'tp', 'mon', 2000, 'full', 'sfc', 'global', tmp_path)
    assert fake_backend == []


def test_month_retrieve(tmp_path, fake_backend):
    out = dl.month_retrieve('ERA5', 'temperature', '1hr', '2000', '03', 'full', 'plev37', 'global', tmp_path)
    assert out == tmp_path / 'ERA5_temperature_1hr_full_plev37_2000_month03.grib'
    assert len(fake_backend) == 1 and fake_backend[0][1]['month'] == ['03']
    # complete chunks are not downloaded again
    dl.month_retrieve('ERA5', 'temperature', '1hr', '2000', '03', 'full', 'plev37', 'global', tmp_path)
    assert len(fake_backend) == 1


def test_concat_months(tmp_path, fake_backend):
    yearfile = tmp_path / 'ERA5_tp_1hr_full_sfc_2000.grib'
    for month in range(1, 12):
        (tmp_path / f'ERA5_tp_1hr_full_sfc_2000_month{month:02d}.grib').write_text('grib')
    with pytest.raises(RuntimeError, match='month12'):
        dl.concat_months('ERA5', 'tp', '1hr', '2000', 'full', 'sfc', 'global', tmp_path)
    assert not yearfile.exists() and len(list(tmp_path.iterdir())) == 11

    (tmp_path / 'ERA5_tp_1hr_full_sfc_2000_month12.grib').write_text('grib')
    out = dl.concat_months('ERA5', 'tp', '1hr', '2000', 'full', 'sfc', 'global', tmp_path)
    assert out == yearfile
    assert [p.name for p in tmp_path.iterdir()] == [yearfile.name]
    assert yearfile.read_text().split()[-1].endswith('_month12.grib')


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
def config():
    return dict(dataset='ERA5', freq='1hr', grid='full', levelout='plev37', area='global',
                product_type='reanalysis', retries=1, nprocs=10)


def test_retrieve_variable_monthly(tmp_path, fake_backend, config, monkeypatch):
    calls = []

    def recording_run(func, jobs, nprocs):
        calls.append((func.__name__, [label for label, _ in jobs]))
        return serial_run(func, jobs, nprocs)

    monkeypatch.setattr(runner, 'run_parallel', recording_run)
    (tmp_path / 'ERA5_t_1hr_full_plev37_2000.grib').write_text('done')
    (tmp_path / 'ERA5_t_1hr_full_plev37_2001_month05.grib').write_text('grib')

    failures = runner.retrieve_variable(config, 't', ['2000', '2001', '2002'], 'monthly', tmp_path)
    assert failures == {}
    # complete years are skipped, all the other months go to the pool at once
    assert calls[0][0] == 'month_retrieve' and len(calls[0][1]) == 24
    assert calls[1] == ('concat_months', ['t 2001 concat', 't 2002 concat'])
    # the complete chunk of May 2001 is not downloaded again
    assert len(fake_backend) == 23
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        f'ERA5_t_1hr_full_plev37_{y}.grib' for y in ('2000', '2001', '2002')]


def test_retrieve_variable_monthly_failure(tmp_path, fake_backend, config, monkeypatch):
    monkeypatch.setattr(runner, 'run_parallel', serial_run)
    original = dl.download

    def flaky_download(name, request, target, retries=3, client=None):
        if request['year'] == ['2001'] and request['month'] == ['07']:
            raise RuntimeError('CDS error')
        original(name, request, target, retries=retries, client=client)

    monkeypatch.setattr(dl, 'download', flaky_download)
    failures = runner.retrieve_variable(config, 't', ['2000', '2001'], 'monthly', tmp_path)
    assert list(failures) == ['t 2001-07 retrieve']
    # the complete year is concatenated, the chunks of the failed one are kept
    assert (tmp_path / 'ERA5_t_1hr_full_plev37_2000.grib').exists()
    assert not (tmp_path / 'ERA5_t_1hr_full_plev37_2001.grib').exists()
    assert len(list(tmp_path.glob('*2001_month*.grib'))) == 11


def test_retrieve_variable_yearly(tmp_path, fake_backend, config, monkeypatch):
    monkeypatch.setattr(runner, 'run_parallel', serial_run)
    assert runner.retrieve_variable(config, 't', ['2000', '2001'], 'yearly', tmp_path) == {}
    assert [r[1]['month'] for r in fake_backend] == [[f'{m:02d}' for m in range(1, 13)]] * 2


def test_download_retries(tmp_path, monkeypatch):
    monkeypatch.setattr(dl.time, 'sleep', lambda s: None)

    class FlakyClient:
        calls = 0

        def retrieve(self, name, request, target):
            self.calls += 1
            Path(target).write_text('partial')
            if self.calls < 2:
                raise ConnectionError('boom')

    client = FlakyClient()
    target = dl.download('x', {}, tmp_path / 'f.grib', retries=3, client=client)
    assert client.calls == 2 and target.read_text() == 'partial'
    assert not (tmp_path / 'f.grib.part').exists()

    class BrokenClient:
        def retrieve(self, name, request, target):
            Path(target).write_text('partial')
            raise ConnectionError('boom')

    with pytest.raises(RuntimeError, match='after 2 attempts'):
        dl.download('x', {}, tmp_path / 'g.grib', retries=2, client=BrokenClient())
    assert list(tmp_path.glob('g.grib*')) == []

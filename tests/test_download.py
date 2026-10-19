from pathlib import Path

import pytest

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

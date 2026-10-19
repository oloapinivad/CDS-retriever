import datetime

import pytest

from cds_retriever.postproc import which_new_years_download, year_range

ARGS = ('ERA5', 'tp', 'mon', 'full', 'sfc')


def test_year_range():
    files = ['/x/ERA5_tp_mon_full_sfc_1940-1990.nc', '/x/ERA5_tp_mon_full_sfc_1991.nc',
             '/x/ERA5_tp_mon_full_sfc_1992_65_-15_25_45.nc']
    assert year_range(files, *ARGS) == ('1940', '1992')
    with pytest.raises(FileNotFoundError):
        year_range(['/x/other.nc'], *ARGS)


def test_which_new_years_download(tmp_path):
    destdir = tmp_path / 'tp' / 'mon'
    destdir.mkdir(parents=True)
    (destdir / 'ERA5_tp_mon_full_sfc_1940-2020.nc').touch()
    year1, year2 = which_new_years_download(tmp_path, *ARGS, 'global')
    assert year1 == 2021
    assert year2 == datetime.datetime.now().year - 1

    with pytest.raises(FileNotFoundError):
        which_new_years_download(tmp_path, 'ERA5', 'tp', '1hr', 'full', 'sfc', 'global')


def test_which_new_years_download_hourly(tmp_path):
    destdir = tmp_path / 'tp' / '1hr'
    destdir.mkdir(parents=True)
    for year in (2018, 2019):
        (destdir / f'ERA5_tp_1hr_full_sfc_{year}.nc').touch()
    assert which_new_years_download(tmp_path, 'ERA5', 'tp', '1hr', 'full', 'sfc', 'global')[0] == 2020

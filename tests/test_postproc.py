import datetime
from pathlib import Path

import pytest

from cds_retriever import postproc
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


def test_update_reports_legacy_area_names(tmp_path):
    """Before 0.2, merged files with an area got the years from the area string"""
    destdir = tmp_path / 'tp' / 'mon'
    destdir.mkdir(parents=True)
    (destdir / 'ERA5_tp_mon_full_sfc_40-40_70_-20_30_40.nc').touch()
    with pytest.raises(FileNotFoundError, match='ERA5_tp_mon_full_sfc_40-40_70_-20_30_40.nc'):
        which_new_years_download(tmp_path, *ARGS, [70, -20, 30, 40])
    (destdir / 'ERA5_tp_mon_full_sfc_40-40_70_-20_30_40.nc').rename(
        destdir / 'ERA5_tp_mon_full_sfc_2022-2023_70_-20_30_40.nc')
    assert which_new_years_download(tmp_path, *ARGS, [70, -20, 30, 40])[0] == 2024


class FakeCdo:
    def daymean(self, input, output, options=None):
        Path(output).write_text(input)


def make_subdaily(tmp_path, years):
    destdir = tmp_path / 'tcc' / '6hrs'
    destdir.mkdir(parents=True, exist_ok=True)
    for year in years:
        (destdir / f'ERA5_tcc_6hrs_full_sfc_{year}.nc').touch()
    daydir = tmp_path / 'tcc' / 'day'
    daydir.mkdir(exist_ok=True)
    return daydir


def test_daily_mean_replaces_old_file(tmp_path, monkeypatch):
    monkeypatch.setattr(postproc, 'get_cdo', FakeCdo)
    daydir = make_subdaily(tmp_path, range(1979, 2026))
    for name in ['ERA5_tcc_day_full_sfc_1979-2019.nc',        # superseded
                 'ERA5_tcc_day_full_sfc_1950-2020.mmsec.nc',  # not following the scheme
                 'ERA5_tcc_day_0.25x0.25_sfc_1979-2019.nc',   # another grid
                 'ERA5_tcc_day_full_plev8_1979-2019.nc']:     # another level
        (daydir / name).touch()
    out = postproc.daily_mean(tmp_path, 'ERA5', 'tcc', '6hrs', 'full', 'sfc', 'global')
    assert out.name == 'ERA5_tcc_day_full_sfc_1979-2025.nc'
    assert sorted(p.name for p in daydir.iterdir()) == [
        'ERA5_tcc_day_0.25x0.25_sfc_1979-2019.nc', 'ERA5_tcc_day_full_plev8_1979-2019.nc',
        'ERA5_tcc_day_full_sfc_1950-2020.mmsec.nc', 'ERA5_tcc_day_full_sfc_1979-2025.nc']


def test_daily_mean_keeps_old_file_with_more_years(tmp_path, monkeypatch):
    """Old yearly files deleted from the archive: the old daily file is the only copy"""
    monkeypatch.setattr(postproc, 'get_cdo', FakeCdo)
    daydir = make_subdaily(tmp_path, range(2015, 2026))
    (daydir / 'ERA5_tcc_day_full_sfc_1979-2019.nc').touch()
    postproc.daily_mean(tmp_path, 'ERA5', 'tcc', '6hrs', 'full', 'sfc', 'global')
    assert sorted(p.name for p in daydir.iterdir()) == [
        'ERA5_tcc_day_full_sfc_1979-2019.nc', 'ERA5_tcc_day_full_sfc_2015-2025.nc']

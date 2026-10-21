import datetime
import shutil
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


@pytest.mark.parametrize('param_id, code', [(167, '167.128'), ('130', '130.128'), (228246, '246.228')])
def test_grib1_param(param_id, code):
    assert postproc.grib1_param(param_id) == code


def cfgrib_netcdf(path, name, param_id, short_name, levels=None):
    """A netCDF file as delivered by the CDS for daily statistics: cfgrib names and attributes"""
    netCDF4 = pytest.importorskip('netCDF4')
    np = pytest.importorskip('numpy')
    lat, lon = np.arange(90, -90.1, -30.0), np.arange(0, 360, 60.0)
    shape = (3,) + ((len(levels),) if levels else ()) + (len(lat), len(lon))
    values = (200 + 100 * np.random.default_rng(0).random(shape)).astype('f4')
    with netCDF4.Dataset(path, 'w') as nc:
        nc.createDimension('valid_time', None)
        time = nc.createVariable('valid_time', 'f8', ('valid_time',))
        time.setncatts({'standard_name': 'time', 'units': 'days since 1940-01-01 00:00:00',
                        'calendar': 'proleptic_gregorian'})
        time[:] = [0, 1, 2]
        dims = ['valid_time']
        if levels:
            nc.createDimension('pressure_level', len(levels))
            plev = nc.createVariable('pressure_level', 'f8', ('pressure_level',))
            plev.setncatts({'standard_name': 'air_pressure', 'units': 'hPa', 'positive': 'down'})
            plev[:] = levels
            dims.append('pressure_level')
        for dim, axis, units in (('latitude', lat, 'degrees_north'), ('longitude', lon, 'degrees_east')):
            nc.createDimension(dim, len(axis))
            var = nc.createVariable(dim, 'f8', (dim,))
            var.setncatts({'standard_name': dim, 'units': units})
            var[:] = axis
            dims.append(dim)
        var = nc.createVariable(name, 'f4', dims, fill_value=np.float32('nan'))
        var.setncatts({'units': 'K', 'GRIB_paramId': param_id, 'GRIB_shortName': short_name})
        var[:] = values
    return values


@pytest.mark.skipif(not shutil.which('cdo'), reason='needs cdo')
@pytest.mark.parametrize('name, param_id, short_name, levels', [('t2m', 167, '2t', None),
                                                                 ('t', 130, 't', [1000, 500])])
def test_year_convert_netcdf_via_grib(tmp_path, name, param_id, short_name, levels):
    """Daily statistics get the names of the converted GRIB files, with the same values"""
    netCDF4 = pytest.importorskip('netCDF4')
    np = pytest.importorskip('numpy')
    infile = tmp_path / 'ERA5_x_day_full_sfc_1940.nc'
    values = cfgrib_netcdf(infile, name, param_id, short_name, levels)
    out = postproc.year_convert(infile, tmp_path / 'out.nc')
    with netCDF4.Dataset(out) as nc:
        assert list(nc.variables[short_name].dimensions) == ['time'] + (['plev'] if levels else []) + ['lat', 'lon']
        np.testing.assert_array_equal(nc.variables[short_name][:], values)
        assert nc.variables['time'][:].tolist() == [0, 1, 2]
        if levels:
            assert nc.variables['plev'][:].tolist() == [100000, 50000]
    assert sorted(p.name for p in tmp_path.iterdir()) == ['ERA5_x_day_full_sfc_1940.nc', 'out.nc']


@pytest.mark.skipif(not shutil.which('cdo'), reason='needs cdo')
def test_year_convert_netcdf_without_grib1_code(tmp_path):
    """A parameter of GRIB2 only (2 metre relative humidity) is an error, not a renamed variable"""
    infile = tmp_path / 'in.nc'
    cfgrib_netcdf(infile, 'r2', 260242, '2r')
    with pytest.raises(ValueError, match='no GRIB1 code for 2r'):
        postproc.year_convert(infile, tmp_path / 'out.nc')
    assert sorted(p.name for p in tmp_path.iterdir()) == ['in.nc']

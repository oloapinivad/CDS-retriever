import pytest

from cds_retriever.request import (build_request, create_filename, dataset_id, define_level,
                                   define_time, download_suffix, expected_steps, level_label,
                                   month_chunks)

MONTHS = [str(i).zfill(2) for i in range(1, 13)]


@pytest.mark.parametrize('levelout, expected', [
    ('sfc', (None, 'single-levels')),
    ('500hPa', (['500'], 'pressure-levels')),
    ('850', (['850'], 'pressure-levels')),
    (['500hPa', '850'], (['500', '850'], 'pressure-levels')),
])
def test_define_level(levelout, expected):
    assert define_level(levelout) == expected


def test_define_level_sets():
    assert len(define_level('plev8')[0]) == 8
    assert len(define_level('plev19')[0]) == 19
    assert len(define_level('plev37')[0]) == 37


@pytest.mark.parametrize('levelout', ['501hPa', 'foo', ['500', 'bar']])
def test_define_level_invalid(levelout):
    with pytest.raises(ValueError):
        define_level(levelout)


@pytest.mark.parametrize('levelout, label', [
    ('sfc', 'sfc'), ('plev19', 'plev19'), ('500hPa', '500hPa'), ('500', '500'),
    (['500hPa'], '500hPa'), (['500hPa', '850'], '500-850hPa'),
])
def test_level_label(levelout, label):
    assert level_label(levelout) == label


@pytest.mark.parametrize('dataset, kind, freq, name', [
    ('ERA5', 'single-levels', '1hr', 'reanalysis-era5-single-levels'),
    ('ERA5', 'pressure-levels', 'mon', 'reanalysis-era5-pressure-levels-monthly-means'),
    ('ERA5-Land', 'single-levels', '6hrs', 'reanalysis-era5-land'),
    ('ERA5-Land', 'single-levels', 'mon', 'reanalysis-era5-land-monthly-means'),
    ('ERA5', 'single-levels', 'day', 'derived-era5-single-levels-daily-statistics'),
    ('ERA5', 'pressure-levels', 'daymax', 'derived-era5-pressure-levels-daily-statistics'),
    ('ERA5-Land', 'single-levels', 'daysum', 'derived-era5-land-daily-statistics'),
])
def test_dataset_id(dataset, kind, freq, name):
    assert dataset_id(dataset, kind, freq) == name


def test_dataset_id_invalid():
    with pytest.raises(ValueError):
        dataset_id('ERA5-Land', 'pressure-levels', 'mon')
    with pytest.raises(ValueError):
        dataset_id('ERA6', 'single-levels', 'mon')
    with pytest.raises(ValueError, match='only on single levels'):
        dataset_id('ERA5', 'pressure-levels', 'daysum')


def test_define_time():
    assert define_time('mon') == ('monthly_averaged_reanalysis', None, ['00:00'])
    assert define_time('mon', 'ensemble_members')[0] == 'monthly_averaged_ensemble_members'
    product, day, time = define_time('3hrs', 'ensemble_members')
    assert product == 'ensemble_members' and len(day) == 31 and len(time) == 8
    assert len(define_time('6hrs')[2]) == 4
    assert len(define_time('1hr')[2]) == 24
    assert define_time('instant')[1:] == (['01'], ['00:00'])
    with pytest.raises(ValueError):
        define_time('1hr', 'ensemble_members')
    with pytest.raises(ValueError):
        define_time('2hrs')
    product, day, time = define_time('day', 'ensemble_mean')
    assert product == 'ensemble_mean' and len(day) == 31 and time is None
    with pytest.raises(ValueError, match='daily statistics'):
        define_time('daymin', 'ensemble_spread')


def test_expected_steps():
    assert expected_steps('mon', 2000, MONTHS) == 12
    assert expected_steps('1hr', 2000, MONTHS) == 366 * 24
    assert expected_steps('6hrs', 2001, MONTHS) == 365 * 4
    assert expected_steps('3hrs', 2001, ['02']) == 28 * 8
    assert expected_steps('day', 2000, MONTHS) == 366
    assert expected_steps('daymax', 2001, ['02']) == 28


def test_month_chunks():
    assert month_chunks('yearly') == [MONTHS]
    assert month_chunks('monthly')[0] == ['01'] and len(month_chunks('monthly')) == 12
    with pytest.raises(ValueError):
        month_chunks('auto')


def test_build_request_monthly_means():
    name, req = build_request('ERA5', '2m_temperature', 'mon', 2000, MONTHS)
    assert name == 'reanalysis-era5-single-levels-monthly-means'
    assert req == {
        'product_type': ['monthly_averaged_reanalysis'],
        'variable': ['2m_temperature'],
        'year': ['2000'],
        'month': MONTHS,
        'time': ['00:00'],
        'data_format': 'grib',
        'download_format': 'unarchived',
    }


@pytest.mark.parametrize('freq, statistic', [('day', 'daily_mean'), ('daymin', 'daily_minimum'),
                                             ('daymax', 'daily_maximum'), ('daysum', 'daily_sum')])
def test_build_request_daily_statistics(freq, statistic):
    name, req = build_request('ERA5', '2m_temperature', freq, 2000, ['01'])
    assert name == 'derived-era5-single-levels-daily-statistics'
    assert req == {
        'product_type': ['reanalysis'],
        'variable': ['2m_temperature'],
        'year': ['2000'],
        'month': ['01'],
        'day': [str(i).zfill(2) for i in range(1, 32)],
        'daily_statistic': statistic,
        'time_zone': 'utc+00:00',
        'frequency': '1_hourly',
    }
    _, req = build_request('ERA5', 'temperature', 'day', 2000, MONTHS, levelout='plev8')
    assert len(req['pressure_level']) == 8
    with pytest.raises(ValueError, match='grid selection'):
        build_request('ERA5', '2m_temperature', 'day', 2000, MONTHS, grid='1x1')


def test_build_request_pressure_levels():
    name, req = build_request('ERA5', 'temperature', '6hrs', ['2000', '2001'], ['01'],
                              grid='2.5x2.5', levelout=['500hPa', '850hPa'], area=[65, -15, 25, 45])
    assert name == 'reanalysis-era5-pressure-levels'
    assert req['pressure_level'] == ['500', '850']
    assert req['grid'] == ['2.5', '2.5']
    assert req['area'] == [65.0, -15.0, 25.0, 45.0]
    assert req['year'] == ['2000', '2001']
    assert len(req['day']) == 31


def test_build_request_era5_land_hourly_has_no_product_type():
    name, req = build_request('ERA5-Land', '2m_temperature', '1hr', 2000, MONTHS)
    assert name == 'reanalysis-era5-land'
    assert 'product_type' not in req


def test_create_filename():
    assert create_filename('ERA5', 'tp', 'mon', 'full', 'sfc', 'global', '1940') == 'ERA5_tp_mon_full_sfc_1940'
    assert create_filename('ERA5', 'tp', 'mon', 'full', 'sfc', 'global', '1940', '2022') == \
        'ERA5_tp_mon_full_sfc_1940-2022'
    assert create_filename('ERA5', 't', '1hr', '2.5x2.5', ['500hPa', '850'], [65, -15, 25, 45], '1940', '2022') == \
        'ERA5_t_1hr_2.5x2.5_500-850hPa_1940_65_-15_25_45'


def test_download_suffix():
    assert download_suffix('mon') == '.grib' and download_suffix('1hr') == '.grib'
    assert download_suffix('day') == '.nc' and download_suffix('daysum') == '.nc'

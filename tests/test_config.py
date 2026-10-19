import pytest

from cds_retriever.config import DEFAULTS, apply_args, load_config, parser, validate_config


@pytest.fixture
def config_file(tmp_path):
    path = tmp_path / 'config.yaml'
    path.write_text(
        "tmpdir: /tmp/a\nstoredir: /tmp/b\nvarlist: total_precipitation\n"
        "year:\n  begin: 1990\n  end: 1991\nfreq: 1hr\n"
    )
    return path


def test_load_and_validate(config_file):
    config = validate_config(load_config(config_file))
    assert config['varlist'] == ['total_precipitation']
    assert config['year'] == {'begin': 1990, 'end': 1991, 'update': False}
    assert config['download_request'] == DEFAULTS['download_request']
    assert config['freq'] == '1hr'


def test_cli_overrides(config_file):
    args = parser(['-c', str(config_file), '-n', '2', '-v', '2m_temperature', '-l', '500hPa', '-u'])
    config = validate_config(apply_args(load_config(config_file), args))
    assert config['nprocs'] == 2
    assert config['varlist'] == ['2m_temperature']
    assert config['levelout'] == '500hPa'
    assert config['year']['update'] is True


def test_validation_errors(config_file):
    config = load_config(config_file)
    config.update(freq='2hrs', grid='3x3', levelout='501hPa', area=[10, 0, 20, 5], nprocs=0)
    config['year'] = {'begin': 2000, 'end': 1990, 'update': False}
    with pytest.raises(ValueError) as err:
        validate_config(config)
    msg = str(err.value)
    for key in ('freq', 'grid', 'levelout', 'area', 'year.begin', 'nprocs'):
        assert key in msg


def test_missing_required(tmp_path):
    path = tmp_path / 'config.yaml'
    path.write_text("freq: mon\n")
    with pytest.raises(ValueError, match='tmpdir is required'):
        validate_config(load_config(path))


def test_template_is_valid():
    from pathlib import Path
    validate_config(load_config(Path(__file__).parents[1] / 'config.tmpl'))

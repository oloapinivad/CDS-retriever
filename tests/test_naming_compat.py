"""File names must stay identical to the original (pre-package) naming scheme,
so that existing archives keep working with the update feature."""

import itertools

import pytest

from cds_retriever.request import create_filename


def legacy_create_filename(dataset, var, freq, grid, levelout, area, year1, year2=None):
    """Verbatim copy of create_filename from CDS_retriever.py on main (commit 558cee7)"""
    filename = dataset + '_' + var + '_' + freq + '_' + grid + '_' + levelout + '_' + year1
    if (freq == 'mon') and (year2 is not None):
        filename = filename + '-' + year2
    if area != 'global':
        strarea = "_".join([str(x) for x in area])
        filename = filename + '_' + strarea
    return filename


CASES = list(itertools.product(
    ['ERA5', 'ERA5-Land'],
    ['total_precipitation', '2m_temperature'],
    ['instant', '1hr', '3hrs', '6hrs', 'mon', 'day'],
    ['full', '0.25x0.25', '2.5x2.5'],
    ['sfc', 'plev8', 'plev19', 'plev37', '500hPa', '500', '850'],
    ['global', [65, -15, 25, 45], [65.5, -15.25, 25, 45]],
    [('1940', None), ('1940', '2022'), ('????', None), ('????', '????'), ('1940-2022', None)],
))


@pytest.mark.parametrize('dataset, var, freq, grid, levelout, area, years', CASES)
def test_same_names_as_legacy(dataset, var, freq, grid, levelout, area, years):
    assert create_filename(dataset, var, freq, grid, levelout, area, *years) == \
        legacy_create_filename(dataset, var, freq, grid, levelout, area, *years)

"""Thin helpers around the CDO python bindings"""

from functools import lru_cache


@lru_cache(maxsize=None)
def get_cdo():
    """Return a Cdo instance, created lazily (one per process)"""
    from cdo import Cdo
    return Cdo()


def result(lines):
    """
    Result of a CDO information operator: its last output line, since CDO may print warnings
    before it (e.g. 'cdi  gribapiGetTsteptype: Time stepType avgd unsupported' for GRIB2 means)
    """
    return lines[-1]


def ntime(filename):
    """Number of time steps in a file, None if CDO cannot read it"""
    from cdo import CDOException
    try:
        return int(result(get_cdo().ntime(input=str(filename), options='-s')))
    except (KeyError, IndexError, ValueError, CDOException):
        return None

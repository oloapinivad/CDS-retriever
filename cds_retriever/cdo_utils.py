"""Thin helpers around the CDO python bindings"""

from functools import lru_cache


@lru_cache(maxsize=None)
def get_cdo():
    """Return a Cdo instance, created lazily (one per process)"""
    from cdo import Cdo
    return Cdo()


def ntime(filename):
    """Number of time steps in a file, None if CDO cannot read it"""
    from cdo import CDOException
    try:
        return int(get_cdo().ntime(input=str(filename), options='-s')[0])
    except (KeyError, IndexError, ValueError, CDOException):
        return None

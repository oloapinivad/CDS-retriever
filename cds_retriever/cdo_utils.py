"""Thin helpers around the CDO python bindings"""

import signal
import types
from functools import lru_cache

# Stand-in for the signal module in python-cdo: in the main thread, its tempfile store (created
# again for each new operator) wraps the SIGINT, SIGTERM and SIGSEGV handlers and makes them
# restart system calls, so that Ctrl-C would wait for the running jobs instead of stopping them.
_NO_SIGNALS = types.SimpleNamespace(SIGINT=signal.SIGINT, SIGTERM=signal.SIGTERM,
                                    SIGSEGV=signal.SIGSEGV, getsignal=signal.getsignal,
                                    signal=lambda sig, handler: None,
                                    siginterrupt=lambda sig, flag: None)


@lru_cache(maxsize=None)
def get_cdo():
    """Return a Cdo instance, created lazily (one per process), leaving the signal handlers alone"""
    from cdo import Cdo, cdo as cdo_module
    cdo_module.signal = _NO_SIGNALS
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

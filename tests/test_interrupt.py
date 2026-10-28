import os
import signal
import subprocess
import threading
import time
from pathlib import Path

import pytest

from cds_retriever import cli, runner
from cds_retriever import download as dl


def slow_job(outdir, n):
    """Record the worker and its subprocess, then wait: the cleanup marks an interrupted job"""
    outdir = Path(outdir)
    child = subprocess.Popen(['sleep', '60'])
    (outdir / f'{n}.pids').write_text(f'{os.getpid()} {child.pid}')
    try:
        time.sleep(60)
    finally:
        (outdir / f'{n}.cleanup').touch()


def is_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # zombies of the main process are not running anymore
    status = Path(f'/proc/{pid}/status')
    return not (status.exists() and 'zombie' in status.read_text())


def test_run_parallel_interrupted(tmp_path):
    """Ctrl-C stops the workers and their subprocesses, after the cleanups of the running jobs"""
    jobs = [(f'job{n}', dict(outdir=str(tmp_path), n=n)) for n in range(5)]

    def interrupt():
        deadline = time.monotonic() + 30
        while len(list(tmp_path.glob('*.pids'))) < 2 and time.monotonic() < deadline:
            time.sleep(0.1)
        os.kill(os.getpid(), signal.SIGINT)  # a real signal, as Ctrl-C: interrupt_main does not wake a wait

    threading.Thread(target=interrupt, daemon=True).start()
    start = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        runner.run_parallel(slow_job, jobs, 2)
    assert time.monotonic() - start < 30

    started = sorted(tmp_path.glob('*.pids'))
    assert len(started) == 2  # pending jobs are cancelled
    pids = [int(p) for f in started for p in f.read_text().split()]
    time.sleep(0.5)
    assert not [pid for pid in pids if is_alive(pid)]
    assert len(list(tmp_path.glob('*.cleanup'))) == 2


class FakeRemote:
    request_id = 'abc'
    deleted = False

    def __init__(self, error):
        self.error = error

    def download(self, target):
        raise self.error

    def delete(self):
        self.deleted = True


class FakeClient:
    """cdsapi >= 0.7 client wrapping an ecmwf.datastores client"""

    def __init__(self, error):
        self.remote = FakeRemote(error)
        self.client = self

    def submit(self, name, request):
        return self.remote


def test_download_interrupted_deletes_request(tmp_path):
    client = FakeClient(KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        dl.download('x', {}, tmp_path / 'f.grib', retries=3, wait=0, client=client)
    assert client.remote.deleted


def test_download_failure_keeps_request(tmp_path):
    client = FakeClient(ConnectionError('boom'))
    with pytest.raises(RuntimeError, match='after 2 attempts'):
        dl.download('x', {}, tmp_path / 'f.grib', retries=2, wait=0, client=client)
    assert not client.remote.deleted


def test_handle_signals(monkeypatch):
    handlers = {}
    monkeypatch.setattr(cli.signal, 'signal', lambda sig, handler: handlers.__setitem__(sig, handler))
    monkeypatch.setattr(cli.signal, 'getsignal', lambda sig: signal.SIG_DFL)
    cli.handle_signals()
    assert set(handlers) == {signal.SIGTERM, signal.SIGHUP}
    with pytest.raises(KeyboardInterrupt):
        handlers[signal.SIGTERM](signal.SIGTERM, None)

    # nohup: SIGHUP stays ignored
    handlers.clear()
    monkeypatch.setattr(cli.signal, 'getsignal', lambda sig: signal.SIG_IGN)
    cli.handle_signals()
    assert set(handlers) == {signal.SIGTERM}


def test_get_cdo_keeps_signal_handlers():
    """python-cdo replaces the handlers and makes them restart system calls, delaying Ctrl-C"""
    pytest.importorskip('cdo')
    from cds_retriever import cdo_utils
    cdo_utils.get_cdo.cache_clear()
    before = signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)
    cdo_utils.get_cdo().nyear  # the first call of an operator creates a new tempfile store
    assert (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)) == before

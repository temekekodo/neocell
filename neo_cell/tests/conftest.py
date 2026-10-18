"""Shared fixtures plus a minimal asyncio runner for the test suite.

The project keeps zero third-party dependencies, and the tests preserve
that: rather than add pytest-asyncio, the hooks here run coroutine tests
and the coroutine ``ppe`` fixture on one event loop per test. The loop
is created and closed around the whole test protocol, because the
process-backed workers spawned by ``ppe.start()`` are bound to it and
must still be torn down while it is alive.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest

from neocell.ppe import DmaModel, Ppe, PpeConfig

SPE_COUNT = 4
STORE_BYTES = 16384

_LOOP: asyncio.AbstractEventLoop | None = None


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "asyncio: run this test on an event loop")


def _get_loop() -> asyncio.AbstractEventLoop:
    global _LOOP
    if _LOOP is None:
        _LOOP = asyncio.new_event_loop()
    return _LOOP


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item: pytest.Item, nextitem: pytest.Item | None):
    global _LOOP
    _LOOP = asyncio.new_event_loop()
    try:
        yield
    finally:
        loop, _LOOP = _LOOP, None
        if not loop.is_closed():
            pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem: pytest.Function):
    test = pyfuncitem.obj
    if not inspect.iscoroutinefunction(test):
        return None
    kwargs = {
        name: pyfuncitem.funcargs[name] for name in pyfuncitem._fixtureinfo.argnames
    }
    _get_loop().run_until_complete(test(**kwargs))
    return True


@pytest.fixture
def ppe_config() -> PpeConfig:
    return PpeConfig(spe_count=SPE_COUNT, local_store_bytes=STORE_BYTES)


@pytest.fixture
def ppe(ppe_config: PpeConfig):
    """A PPE with four SPE child processes, started on the test's loop."""
    loop = _get_loop()
    coordinator = Ppe(ppe_config, dma=DmaModel())
    loop.run_until_complete(coordinator.start())
    try:
        yield coordinator
    finally:
        loop.run_until_complete(coordinator.stop())
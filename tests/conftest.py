"""Pytest setup: keep test flow runs out of Prefect Cloud.

Tests execute real ``@flow`` functions (e.g. ``run_plan`` in
``tests/test_nyc_taxi.py``). Without this fixture every test run would register
a flow and flow run in the workspace configured in the active profile. The
harness points the client at a temporary local server instead.
"""

import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(scope="session", autouse=True)
def ephemeral_prefect_server():
    with prefect_test_harness():
        yield

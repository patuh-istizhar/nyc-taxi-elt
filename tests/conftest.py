import os

import pytest
from prefect.testing.utilities import prefect_test_harness

# Tests must never use your real .env or prod settings.
for name in list(os.environ):
    if name.startswith(("S3_", "WAREHOUSE_", "MOTHERDUCK_")):
        del os.environ[name]
os.environ.update(ENV="dev", S3_ACCESS_KEY_ID="test", S3_SECRET_ACCESS_KEY="test")


@pytest.fixture(scope="session", autouse=True)
def prefect_test_server():
    """Run flows against a throwaway Prefect server, not your Prefect Cloud."""
    with prefect_test_harness():
        yield

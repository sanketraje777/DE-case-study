import os
import sys
import pytest
from airflow.models import Variable

# 1. Ensure Python can import `dags/common/...` by prepending the `dags/` folder:
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DAGS_FOLDER = os.path.join(PROJECT_ROOT, "dags")
if DAGS_FOLDER not in sys.path:
    sys.path.insert(0, DAGS_FOLDER)

# 2. Monkey‐patch Variable.get so pytest/DagBag never tries to talk to a real Airflow DB:
@pytest.fixture(autouse=True)
def patch_variable_get(monkeypatch):
    """
    Replace Variable.get(...) with a lambda that always returns default_var,
    so no Airflow metadata DB is needed when importing DAGs under pytest.
    """
    monkeypatch.setattr(
        Variable,
        "get",
        lambda key, default_var=None, deserialize_json=False, *args, **kwargs: default_var
    )

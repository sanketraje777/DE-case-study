import pytest
from airflow.models import DagBag

@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)

@pytest.mark.parametrize("dag_id", ["etl_landing", "etl_ods", "etl_dm"])
def test_dag_loaded(dagbag, dag_id):
    # If there was any import‐time error, dagbag.import_errors[dag_id] will be non‐empty
    import_errors = dagbag.import_errors.get(dag_id)
    if import_errors:
        pytest.fail(f"DAG {dag_id} import error:\n{import_errors}")

    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} failed to load"

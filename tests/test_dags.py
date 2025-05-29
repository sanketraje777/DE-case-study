import pytest
from airflow.models import DagBag

@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)

@ pytest.mark.parametrize("dag_id", ["etl_landing", "etl_ods", "etl_datamart"])
def test_dag_loaded(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} failed to load"

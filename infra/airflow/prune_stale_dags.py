import logging
import os

from airflow.api.common.delete_dag import delete_dag
from airflow.configuration import conf
from airflow.dag_processing.dagbag import DagBag
from airflow.models.dag import DagModel
from airflow.utils.session import create_session
from sqlalchemy import select


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def main() -> None:
    dags_folder = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER") or conf.get("core", "dags_folder")
    dagbag = DagBag(dag_folder=dags_folder, include_examples=False)
    local_dag_ids = set(dagbag.dags)

    with create_session() as session:
        db_dag_ids = set(session.scalars(select(DagModel.dag_id)).all())
        stale_dag_ids = sorted(db_dag_ids - local_dag_ids)

        if not stale_dag_ids:
            logging.info("No stale DAGs found in Airflow metadata DB.")
            return

        logging.info("Pruning %d stale DAG(s): %s", len(stale_dag_ids), ", ".join(stale_dag_ids))

        for dag_id in stale_dag_ids:
            deleted_count = delete_dag(dag_id=dag_id, session=session)
            logging.info("Pruned stale DAG %s (%d record(s) removed).", dag_id, deleted_count)


if __name__ == "__main__":
    main()

# author: aprilia.titisari
# maintainer:aprilia.titisari

from datetime import datetime, timedelta
from airflow.models import BaseOperator, Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, ShortCircuitOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
# import DAG 
from lib.dbt import DbtBuildOperator
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

slack_error_email_address = Variable.get("slack_error_email_address")
project_id = Variable.get("project")
personal_env_prefix = Variable.get("personal_env_prefix")
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": [slack_error_email_address],
}

DAG_NAME = "dashboard_scheduler"
DS_NODASH = "{{ ds_nodash }}"
ENV = Variable.get("env")
env_image = {"sandbox": "dev-latest", "staging": "staging-latest", "production": "prod-latest"}
DATASET_NAME = "dashboard"

TABLES = [
    "ex_table_1",
]

BRANCHES = [
    "ex_table_2",
]


def table_validation(
    tables: list,
    table_id: str,
) -> bool:
    does_valid_table_name = False

    if table_id in tables:
        does_valid_table_name = True

    return does_valid_table_name


with JDAG(
    dag_id=DAG_NAME,
    schedule_interval="30 5 * * *", # run at 5.30 AM UTC daily https://crontab.guru/#30_5_*_*_*
    start_date=datetime(2021, 1, 1),
    catchup=False,
    description="Daily dbt",
    max_active_runs=1,
    default_args=default_args,
) as dag:
    dbt_vars = {"date_nodash": DS_NODASH}

    task_lists = [
        "master_db_profile_segment",
        "customer_segmented",
    ]
    task_1 = DbtBuildOperator(
        dbt_select=f"{DATASET_NAME}__{task_lists[0]}",
        dbt_vars=dbt_vars,
        airflow_task_id_suffix=f"{task_lists[0]}",
        gcr_image_tag=env_image.get(ENV, "dev-latest"),
    )

    task_2 = DbtBuildOperator(
        dbt_select=f"{DATASET_NAME}__{task_lists[1]}",
        dbt_vars=dbt_vars,
        airflow_task_id_suffix=f"{task_lists[1]}",
        gcr_image_tag=env_image.get(ENV, "dev-latest"),
    )
    (
        task_1
        >> task_2
    )

    join = EmptyOperator(
        task_id="join",
        trigger_rule="all_done",
    )
    for table_id in TABLES:
        does_valid_table_name_first_phase = ShortCircuitOperator(
            task_id=f"branching_{table_id}",
            python_callable=table_validation,
            op_kwargs={
                "table_id": table_id,
                "tables": TABLES,
            },
        )
        dbt_group_run_first_phase = DbtBuildOperator(
            dbt_select=f"{DATASET_NAME}__{table_id}",
            dbt_vars=dbt_vars,
            airflow_task_id_suffix=f"{table_id}",
            gcr_image_tag=env_image.get(ENV, "dev-latest"),
        )
        task_2 >> does_valid_table_name_first_phase >> dbt_group_run_first_phase >> join

    for table_id in BRANCHES:
        does_valid_table_name = ShortCircuitOperator(
            task_id=f"branching_{table_id}",
            python_callable=table_validation,
            op_kwargs={
                "table_id": table_id,
                "tables": BRANCHES,
            },
        )
        dbt_group_run = DbtBuildOperator(
            dbt_select=f"{DATASET_NAME}__{table_id}",
            dbt_vars=dbt_vars,
            airflow_task_id_suffix=f"{table_id}",
            gcr_image_tag=env_image.get(ENV, "dev-latest"),
        )
        join >> does_valid_table_name >> dbt_group_run

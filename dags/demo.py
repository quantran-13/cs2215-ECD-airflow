import datetime as dt

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from thirdparty.clearml.batch_prediction_pipeline import batch_predict_batch_pipeline
from thirdparty.clearml.feature_pipeline import (
    extract_feature_pipeline,
    load_feature_pipeline,
    transform_feature_pipeline,
    validate_feature_pipeline,
)
from thirdparty.clearml.training_pipeline import hpo_training_pipeline, train_training_pipeline

default_args = {
    "depends_on_past": True,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
    "start_date": dt.datetime(2023, 1, 1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    "end_date": dt.datetime(2023, 2, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function, # or list of functions
    # 'on_success_callback': some_other_function, # or list of functions
    # 'on_retry_callback': another_function, # or list of functions
    # 'sla_miss_callback': yet_another_function, # or list of functions
    "trigger_rule": "all_success",
    "max_active_runs": 1,
    "catchup": False,
}

dag = DAG(
    "feature_pipeline",
    default_args=default_args,
    description="Feature pipeline",
    schedule=dt.timedelta(days=7),
    catchup=True,
    tags=["feature-pipeline"],
)

days_delay = int(Variable.get("ml_pipeline_days_delay", default_var=15))
days_export = int(Variable.get("ml_pipeline_days_export", default_var=30))
feature_group_version = str(Variable.get("ml_pipeline_feature_group_version", default_var="1.0"))

t1 = PythonOperator(
    task_id="extract",
    python_callable=extract_feature_pipeline,
    dag=dag,
    op_kwargs={
        "artifacts_task_id": "3dfe30f7f8ca4619b535e43f64f66d05",
        "days_delay": days_delay,
        "days_export": days_export,
    },
)

t2 = PythonOperator(task_id="transform", python_callable=transform_feature_pipeline, dag=dag)
t3 = PythonOperator(task_id="validate", python_callable=validate_feature_pipeline, dag=dag)
t4 = PythonOperator(
    task_id="load",
    python_callable=load_feature_pipeline,
    dag=dag,
    op_kwargs={"feature_group_version": feature_group_version},
)

t5 = PythonOperator(task_id="hpo", python_callable=hpo_training_pipeline, dag=dag)
t6 = PythonOperator(task_id="train", python_callable=train_training_pipeline, dag=dag)
t7 = PythonOperator(task_id="predict", python_callable=batch_predict_batch_pipeline, dag=dag)

t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7

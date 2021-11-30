from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(dag_id='ELT-python-snowflake', schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False, tags=['elt', 'snowflake']) as dag:
    
    mysql_to_s3 = BashOperator(
        task_id='mysql_to_s3',
        bash_command='python3 /opt/airflow/elt/user-orders/extract-load/mysql_to_s3.py',
    )

    with TaskGroup("stream_to_current_group") as stream_to_current_group:

        stream_to_current = BashOperator(
            task_id='stream_to_current',
            bash_command='sleep 30 && python3 /opt/airflow/elt/user-orders/transform-snowflake/1_stream_to_current/stream_to_current.py',
        )

        current_to_audit = BashOperator(
            task_id='current_to_audit',
            bash_command='python3 /opt/airflow/elt/user-orders/transform-snowflake/1_stream_to_current/current_to_audit.py',
        )

        stream_to_current >> current_to_audit

    deltacurrent_to_dedup = BashOperator(
        task_id='deltacurrent_to_dedup',
        bash_command='python3 /opt/airflow/elt/user-orders/transform-snowflake/2_current_to_dedup/current_to_dedup.py',
    )

    with TaskGroup("dedup_to_extract") as dedup_to_extract:
        dedup_to_extractcast = BashOperator(
            task_id='dedup_to_extractcast',
            bash_command='python3 /opt/airflow/elt/user-orders/transform-snowflake/3_dedup_to_extract/dedup_to_extract.py',
        )

        check_extract = BashOperator(
            task_id='check_extract',
            bash_command='python3 /opt/airflow/elt/user-orders/transform-snowflake/3_dedup_to_extract/check_to_extract.py',
        )

        dedup_to_extractcast >> check_extract

    with TaskGroup("data_model") as data_model_group:

        datamodel_dimuser = BashOperator(
            task_id='datamodel_dimuser',
            bash_command='python3 /opt/airflow/elt/user-orders/transform-snowflake/6_enrich_to_datamodel/extract_to_dimuser.py',
        )

        datamodel_dimstatus = BashOperator(
            task_id='datamodel_dimstatus',
            bash_command='python3 /opt/airflow/elt/user-orders/transform-snowflake/6_enrich_to_datamodel/extract_to_dimstatus.py',
        )

        datamodel_factorder = BashOperator(
            task_id='datamodel_factorder',
            bash_command='python3 /opt/airflow/elt/user-orders/transform-snowflake/6_enrich_to_datamodel/extract_to_factorderpy',
        )

        [datamodel_dimuser, datamodel_dimstatus] >> datamodel_factorder
    

mysql_to_s3 >> stream_to_current_group >> deltacurrent_to_dedup >> dedup_to_extract >> data_model_group

if __name__ == "__main__":
    dag.cli()
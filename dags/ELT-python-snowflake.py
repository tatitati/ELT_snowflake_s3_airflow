from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id='ELT-python-snowflake', schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False, tags=['elt', 'snowflake']) as dag:
    
    extract = BashOperator(
        task_id='Extract',
        bash_command='python3 /opt/airflow/elt/user-orders/extract-load/mysql_to_s3.py',
    )

    streamcopy_to_deltacurrent = BashOperator(
        task_id='streamcopy_to_deltacurrent',
        bash_command='sleep 30 && python3 /opt/airflow/elt/user-orders/transform-snowflake/1_stream_to_current/stream_to_current.py',
    )

    deltacurrent_to_dedup = BashOperator(
        task_id='deltacurrent_to_dedup',
        bash_command='python3 /opt/airflow/elt/user-orders/transform-snowflake/2_current_to_dedup/current_to_dedup.py',
    )

    dedup_to_extractcast = BashOperator(
        task_id='dedup_to_extractcast',
        bash_command='python3 /opt/airflow/elt/user-orders/transform-snowflake/3_dedup_to_extract/dedup_to_extract.py',
    )

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
    

extract >> streamcopy_to_deltacurrent >> deltacurrent_to_dedup >> dedup_to_extractcast >> datamodel_dimuser >> datamodel_dimstatus >> datamodel_factorder

if __name__ == "__main__":
    dag.cli()
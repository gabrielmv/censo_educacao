from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Censo Spark Jobs Pipeline',
    'depend_on_past'        : False,
    'start_date'            : datetime(2018, 1, 3),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 1,
    'retry_delay'           : timedelta(seconds=20)
}

parameters_matricula_co_json = """
{
    "source_path": "/lake/raw/microdados_educacao_basica_2020/DADOS/matricula_co.CSV",
    "target_path": "/lake/stage/censo_educacao/matricula_co",
    "mode": "csv"
}
"""

parameters_matricula_norte_json = """
{
    "source_path": "/lake/raw/microdados_educacao_basica_2020/DADOS/matricula_norte.CSV",
    "target_path": "/lake/stage/censo_educacao/matricula_norte",
    "mode": "csv"
}
"""

parameters_matricula_nordeste_json = """
{
    "source_path": "/lake/raw/microdados_educacao_basica_2020/DADOS/matricula_nordeste.CSV",
    "target_path": "/lake/stage/censo_educacao/matricula_nordeste",
    "mode": "csv"
}
"""

parameters_matricula_sudeste_json = """
{
    "source_path": "/lake/raw/microdados_educacao_basica_2020/DADOS/matricula_sudeste.CSV",
    "target_path": "/lake/stage/censo_educacao/matricula_sudeste",
    "mode": "csv"
}
"""

parameters_matricula_sul_json = """
{
    "source_path": "/lake/raw/microdados_educacao_basica_2020/DADOS/matricula_sul.CSV",
    "target_path": "/lake/stage/censo_educacao/matricula_sul",
    "mode": "csv"
}
"""

parameters_states_and_cities_json = """
{
    "source_path": "/lake/raw/microdados_educacao_basica_2020/ANEXOS/ANEXO I - Dicionаrio de Dados e Tabelas Auxiliares/Tabelas Auxiliares.xlsx",
    "target_path": "/lake/stage/censo_educacao/states_and_cities",
    "mode": "xlsx",
    "sheet_name" = "Anexo10 - UFS e Municípios",
    "skiprows = "3"
}
"""

parameters_enrollment_stages_json = """
{
    "source_path": "/lake/raw/microdados_educacao_basica_2020/ANEXOS/ANEXO I - Dicionаrio de Dados e Tabelas Auxiliares/Tabelas Auxiliares.xlsx", 
    "target_path": "/lake/stage/censo_educacao/enrollment_stages",
    "mode": "xlsx", 
    "sheet_name": "Anexo7 - Tabelas Etapas", 
    "skiprows": "6"
}
"""

parameters_stage_to_curated_json = """
{
    "source_path": "/lake/stage/censo_educacao/",
    "target_path": "/lake/curated/censo_educacao/enrollments",
    "tables": [
        "matricula_co",
        "matricula_norte",
        "matricula_nordeste",
        "matricula_sudeste",
        "matricula_sul"
    ],
    "aux_tables": [
        "states_and_cities"
    ]
}
"""

parameters_curated_to_dw_json = """
{
    "source_path": "/lake/curated/censo_educacao/enrollments", 
    "database_url": "jdbc:postgresql://postgresql:5432/postgres", 
    "table": "censo", 
    "user": "postgres", 
    "password": "postgres"
}
"""


with DAG(
    'censo_spark_dag', 
    default_args=default_args, 
    schedule_interval="5 * * * *",
    catchup=False,
    tags=['censo']
) as dag:
    t1 = SparkSubmitOperator(
        application='/opt/bitnami/airflow/dags/src/raw_to_stage.py',
        name='raw_to_stage-matricula_co',
        conn_id='spark_default', 
        task_id='raw_to_stage-matricula_co',
        env_vars={"PARAMETERS": parameters_matricula_co_json},
    )

    t2 = SparkSubmitOperator(
        application='/opt/bitnami/airflow/dags/src/raw_to_stage.py',
        name='raw_to_stage-matricula_norte',
        conn_id='spark_default',
        task_id='raw_to_stage-matricula_norte',
        env_vars={"PARAMETERS": parameters_matricula_norte_json},
    )

    t3 = SparkSubmitOperator(
        application='/opt/bitnami/airflow/dags/src/raw_to_stage.py',
        name='raw_to_stage-matricula_nordeste',
        conn_id='spark_default',
        task_id='raw_to_stage-matricula_nordeste',
        env_vars={"PARAMETERS": parameters_matricula_nordeste_json},
    )

    t4 = SparkSubmitOperator(
        application='/opt/bitnami/airflow/dags/src/raw_to_stage.py',
        name='raw_to_stage-matricula_sudeste',
        conn_id='spark_default',
        task_id='raw_to_stage-matricula_sudeste',
        env_vars={"PARAMETERS": parameters_matricula_sudeste_json},
    )

    t5 = SparkSubmitOperator(
        application='/opt/bitnami/airflow/dags/src/raw_to_stage.py',
        name='raw_to_stage-matricula_sul',
        conn_id='spark_default',
        task_id='raw_to_stage-matricula_sul',
        env_vars={"PARAMETERS": parameters_matricula_sul_json},
    )

    t6 = SparkSubmitOperator(
        application='/opt/bitnami/airflow/dags/src/raw_to_stage.py',
        name='raw_to_stage-states_and_cities',
        conn_id='spark_default',
        task_id='raw_to_stage-states_and_cities',
        env_vars={"PARAMETERS": parameters_states_and_cities_json},
    )

    t7 = SparkSubmitOperator(
        application='/opt/bitnami/airflow/dags/src/raw_to_stage.py',
        name='raw_to_stage-enrollment_stages',
        conn_id='spark_default',
        task_id='raw_to_stage-enrollment_stages',
        env_vars={"PARAMETERS": parameters_enrollment_stages_json},
    )

    t8 = SparkSubmitOperator(
        application='/opt/bitnami/airflow/dags/src/stage_to_curated.py',
        name='stage_to_curated',
        conn_id='spark_default',
        task_id='stage_to_curated',
        env_vars={"PARAMETERS": parameters_stage_to_curated_json},
    )

    t9 = SparkSubmitOperator(
        application='/opt/bitnami/airflow/dags/src/curated_to_dw.py',
        name='curated_to_dw',
        conn_id='spark_default',
        task_id='curated_to_dw',
        env_vars={"PARAMETERS": parameters_curated_to_dw_json},
    )

    t1 >> t8
    t2 >> t8
    t3 >> t8
    t4 >> t8
    t5 >> t8
    t6 >> t8
    t7 >> t8
    t8 >> t9

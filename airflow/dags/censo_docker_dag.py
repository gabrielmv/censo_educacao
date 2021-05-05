from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator


default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Use of the DockerOperator',
    'depend_on_past'        : False,
    'start_date'            : datetime(2018, 1, 3),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 1,
    'retry_delay'           : timedelta(minutes=5)
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
    'censo_docker_dag', 
    default_args=default_args, 
    schedule_interval="5 * * * *",
    catchup=False,
    tags=['censo']
) as dag:
    t1 = DockerOperator(
        task_id='raw_to_stage-matricula_co',
        image='raw_to_stage',
        environment={"PARAMETERS": parameters_matricula_co_json},
        volumes=["/home/gabriel/Documents/trybe/lake:/lake"],
        auto_remove=True,
    )

    t2 = DockerOperator(
        task_id='raw_to_stage-matricula_norte',
        image='raw_to_stage',
        environment={"PARAMETERS": parameters_matricula_norte_json},
        volumes=["/home/gabriel/Documents/trybe/lake:/lake"],
        auto_remove=True,
    )

    t3 = DockerOperator(
        task_id='raw_to_stage-matricula_nordeste',
        image='raw_to_stage',
        environment={"PARAMETERS": parameters_matricula_nordeste_json},
        volumes=["/home/gabriel/Documents/trybe/lake:/lake"],
        auto_remove=True,
    )

    t4 = DockerOperator(
        task_id='raw_to_stage-matricula_sudeste',
        image='raw_to_stage',
        environment={"PARAMETERS": parameters_matricula_sudeste_json},
        volumes=["/home/gabriel/Documents/trybe/lake:/lake"],
        auto_remove=True,
    )

    t5 = DockerOperator(
        task_id='raw_to_stage-matricula_sul',
        image='raw_to_stage',
        environment={"PARAMETERS": parameters_matricula_sul_json},
        volumes=["/home/gabriel/Documents/trybe/lake:/lake"],
        auto_remove=True,
    )

    t6 = DockerOperator(
        task_id='raw_to_stage-states_and_cities',
        image='raw_to_stage',
        environment={"PARAMETERS": parameters_states_and_cities_json},
        volumes=["/home/gabriel/Documents/trybe/lake:/lake"],
        auto_remove=True,
    )

    t7 = DockerOperator(
        task_id='raw_to_stage-enrollment_stages',
        image='raw_to_stage',
        environment={"PARAMETERS": parameters_enrollment_stages_json},
        volumes=["/home/gabriel/Documents/trybe/lake:/lake"],
        auto_remove=True,
    )

    t8 = DockerOperator(
        task_id='stage_to_curated',
        image='stage_to_curated',
        environment={"PARAMETERS": parameters_stage_to_curated_json},
        volumes=["/home/gabriel/Documents/trybe/lake:/lake"],
        auto_remove=True,
    )

    t9 = DockerOperator(
        task_id='curated_to_dw',
        image='curated_to_dw',
        environment={"PARAMETERS": parameters_curated_to_dw_json},
        volumes=["/home/gabriel/Documents/trybe/lake:/lake"],
        auto_remove=True,
    )

    t1 >> t8
    t2 >> t8
    t3 >> t8
    t4 >> t8
    t5 >> t8
    t6 >> t8
    t7 >> t8
    t8 >> t9

#!/bin/bash

# Run Database
docker-compose up -d

# Build Images
sudo docker build -f pipeline/Dockerfile-raw -t raw_to_stage ./pipeline
sudo docker build -f pipeline/Dockerfile-stage -t stage_to_curated ./pipeline
sudo docker build -f pipeline/Dockerfile-curated -t curated_to_dw ./pipeline

# Ingestion of the Data
mkdir lake
mkdir lake/raw
wget https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2020.zip
unzip microdados_censo_escolar_2020.zip -d lake/raw
rm microdados_censo_escolar_2020.zip

# Run raw Jobs
docker run -it \
-v /home/gabriel/Documents/trybe/lake:/lake \
-e PARAMETERS='{"source_path": "/lake/raw/microdados_educacao_basica_2020/DADOS/matricula_co.CSV", "target_path": "/lake/stage/censo_educacao/matricula_co", "mode": "csv"}' \
raw_to_stage

docker run -it \
-v /home/gabriel/Documents/trybe/lake:/lake \
-e PARAMETERS='{"source_path": "/lake/raw/microdados_educacao_basica_2020/DADOS/matricula_norte.CSV", "target_path": "/lake/stage/censo_educacao/matricula_norte", "mode": "csv"}' \
raw_to_stage

docker run -it \
-v /home/gabriel/Documents/trybe/lake:/lake \
-e PARAMETERS='{"source_path": "/lake/raw/microdados_educacao_basica_2020/DADOS/matricula_nordeste.CSV", "target_path": "/lake/stage/censo_educacao/matricula_nordeste", "mode": "csv"}' \
raw_to_stage

docker run -it \
-v /home/gabriel/Documents/trybe/lake:/lake \
-e PARAMETERS='{"source_path": "/lake/raw/microdados_educacao_basica_2020/DADOS/matricula_sudeste.CSV", "target_path": "/lake/stage/censo_educacao/matricula_sudeste", "mode": "csv"}' \
raw_to_stage

docker run -it \
-v /home/gabriel/Documents/trybe/lake:/lake \
-e PARAMETERS='{"source_path": "/lake/raw/microdados_educacao_basica_2020/DADOS/matricula_sul.CSV", "target_path": "/lake/stage/censo_educacao/matricula_sul", "mode": "csv\"}' \
raw_to_stage

docker run -it \
-v /home/gabriel/Documents/trybe/lake:/lake \
-e PARAMETERS='{"source_path": "/lake/raw/microdados_educacao_basica_2020/ANEXOS/ANEXO I - Dicionаrio de Dados e Tabelas Auxiliares/Tabelas Auxiliares.xlsx", "target_path": "/lake/stage/censo_educacao/states_and_cities", "mode": "xlsx", "sheet_name": "Anexo10 - UFS e Municípios", "skiprows": "3"}' \
raw_to_stage

docker run -it \
-v /home/gabriel/Documents/trybe/lake:/lake \
-e PARAMETERS='{"source_path": "/lake/raw/microdados_educacao_basica_2020/ANEXOS/ANEXO I - Dicionаrio de Dados e Tabelas Auxiliares/Tabelas Auxiliares.xlsx", "target_path": "/lake/stage/censo_educacao/enrollment_stages", "mode": "xlsx", "sheet_name": "Anexo7 - Tabelas Etapas", "skiprows": "6"}' \
raw_to_stage

# Run stage jobs
docker run -it \
-v /home/gabriel/Documents/trybe/lake:/lake \
-e PARAMETERS='{"source_path": "/lake/stage/censo_educacao/", "target_path": "/lake/curated/censo_educacao/enrollments", "tables": ["matricula_co", "matricula_norte", "matricula_nordeste", "matricula_sudeste", "matricula_sul" ], "aux_tables": ["states_and_cities", "enrollment_stages"]}' \
stage_to_curated

# Run Curated Jobs
docker run -it \
-v /home/gabriel/Documents/trybe/lake:/lake \
-e PARAMETERS='{"source_path": "/lake/curated/censo_educacao/enrollments", "database_url": "jdbc:postgresql://postgresql:5432/postgres", "table": "censo", "user": "postgres", "password": "postgres"}' \
--network=censo \
curated_to_dw

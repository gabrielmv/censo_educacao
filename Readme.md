# Censo Escolar

Simulação de pipeline para processamento dos dados do Censo Escolar de 2020 do INEP.

Essa pipeline será implementada toda num formato On-premisses. Ela poderia ser implementada na AWS com os serviços indicados, porém não havia billing disponível no momento da criação.

## Arquitertura

A arquitetura definida consiste em um data lake de 3 etapas: raw, stage e curated; um Data Warehouse (representado por um banco PostgreSQL) e jobs que processam os dados entre os estágios do data lake.

![Arquitetura](/images/architecture.png)

### Estagios do Data Lake

O datalake foi simulado utilizando diretórios no próprio sistema na estrutura abaixo

```
lake
|--- raw
|--- stage
|--- curated
```

Cada camada do lake foi pensada para conter dados de uma forma específica:

#### raw

Contém os dados brutos, baixados e descompactados diretamente do INEP, portanto os dados nesta camada estão principalmente em csv e xlsx.

#### stage

Contém dados em processamento, principalmente no formato parquet, subcamadas poderiam ser utilizadas para processamento de dados aqui, mas não foram necessárias.

#### curated

Contém os dados já processados, também em formato parquet. Os dados aqui já estão prontos para serem consumidos ou carregados no DW.

### Jobs

Os jobs abaixo foram escritos utilizando pyspark e são executados em docker. Esse formato de execução foi escolhido para evitar interferências de bibliotecas do sistema host e para conseguir uma portabilidade maior do código. Como são criadas imagens docker parametrizáveis os jobs podem ser executados tanto localmente quanto em serviços como o AWS Batch.

![Arquitetura](/images/job_flow.png)

#### raw-to-stage.py

Converte os dados de CSV para Parquet e faz inferência do schema dos dados brutos.
Parquet já é um formato colunar o que ajudará nas próximas etapas de processamento e garantirá um padrão para o data lake

#### stage-to-curated.py

Realiza uma pequena limpeza nos dados e cria a tabela que será carregada no data warehouse. Foi escolhida a abordagem de criar apenas uma grande tabela em vez de utilizar um esquema estrela. Isso evita que os joins sejam realizados no data warehouse onde a capacidade de processamento é mais limitada que no spark.

#### curated-to-dw.py

Esse job realiza a carga dos dados da curated no dw para que eles sejam consumidos.

### Data Warehouse

O "Data Warehouse" utilizado é um banco de dados PostgreSQL, não foi realizada nenhuma modificação para deixa-lo mais preparado para cargas analíticas para que o projeto seja portável. Para uma implementação em nuvem recomenda-se o uso do AWS Athena ou do AWS Redshift (Compatível com drivers do PostgreSQL) na AWS, BigQuery no GCP ou Synapse Analytics na Azure.

Foi carregada apenas a tabela censo no banco com os dados consolidados.

#### Schema
Para uma maior facilidade, todo schema da tabela foi considerado string:

```
table_name|column_name              |data_type|
----------|-------------------------|---------|
censo     |student_id               |text     |
censo     |enrollment_id            |text     |
censo     |student_age              |text     |
censo     |student_gender           |text     |
censo     |student_ethnicity        |text     |
censo     |has_special_needs        |text     |
censo     |is_vision_impaired       |text     |
censo     |is_blind                 |text     |
censo     |is_hearing_impaired      |text     |
censo     |is_physically_handicapped|text     |
censo     |is_intellectual_disabled |text     |
censo     |is_deaf                  |text     |
censo     |is_deaf_blind            |text     |
censo     |is_multiple_impaired     |text     |
censo     |is_autist                |text     |
censo     |is_gifted                |text     |
censo     |enrollment_stage_id      |text     |
censo     |enrollment_stage_name    |text     |
censo     |region_id                |text     |
censo     |region_name              |text     |
censo     |state_id                 |text     |
censo     |state_name               |text     |
censo     |state_abrv               |text     |
censo     |city_id                  |text     |
censo     |city_name                |text     |
```

### Orquestração

Para um data lake bem estruturado é necessário alguma forma de orquestração para que os jobs executem em um momento certo e na ordem certa. Como essa pipeline é simples a orquestração utilizada foi um simples Script bash que executa todos os passos em ordem. Para executar a pipeline execute:

```
bash run.sh
```

que será realizado o download dos dados, a criação do banco de dados e a execução da pipeline. É necessário que Docker e Docker Compose estejam instalados e sejam executáveis sem privilégios.

Foi iniciada a implementação de DAGs no Apache Airflow para a orquestração, porém a implementação não foi finalizada. As opções de DAGs testadas utilizam ou DockerOperator ou SparkSubmitOperator. 

## Arquitetura Recomendada

Caso os dados fossem 10000 vezes maiores não seria recomendável o uso dessa mesma arquitetura pois ocorreriam problemas no processamento e limitações de memoria. Esses jobs foram executados em uma máquina com 8 Threads e 16 GB de Memória RAM, hardware que provavelmente não seria suficiente.

O recomendável seria migrar para uma arquitetura em núvem similar a abaixo, como a arquitetura é portável seria possível utilizar as mesmas imagens docker, adicionando apenas as bibliotecas da AWS:

![Arquitetura](/images/architecture_cloud.png)

## Pequenas Análises

Foram realizadas duas análises com os resultados exibidos abaixo. As queries estão no diretório queries.

Quais são os 10 municípios que têm o maior número de pessoas no “Ensino Fundamental de 9 anos - 9º Ano”?

![Arquitetura](/images/analysis1.png)

Qual a distribuição de cores/raças (Branca, Pretas, Pardas, Amarelas e Indígenas) entre os estados?

![Arquitetura](/images/analysis2.png)

## Known Issues

A principal melhoria no projeto seria na orquestração, foi realizada algumas tentativas de execução com airflow, destacando-se uma utilizando DockerOperator e outra utilizando SparkSubmitOperator. Os problemas encontrados foram principalmente na comunicação entre o Airflow e a Docker Engine utilizando Docker in Docker e na comunicação do airflow com o Spark cluster também criado em docker.

Outra melhoria é melhorar o schema e nao manter todas as colunas como string e sim com os datatypes mais adequados.

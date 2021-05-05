from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import json
from os import environ as env

spark = SparkSession.builder.appName("stage_to_curated").getOrCreate()

parameters = json.loads(env["PARAMETERS"])

source_path = parameters["source_path"]
target_path = parameters["target_path"]
tables = parameters["tables"]
aux_tables = parameters["aux_tables"]

dfs = {}
for table in tables:
    dfs[table] = spark.read.parquet(source_path + table)
    
aux_dfs = {}
for table in aux_tables:
    aux_dfs[table] = spark.read.parquet(source_path + table)

aux_dfs["region"] = aux_dfs["states_and_cities"].select(
    col("Cdigo_da_Regio").alias("region_id"),
    col("Nome_da_Regio").alias("region_name")
).dropDuplicates()\
.filter(~col("region_id").like("CO_%"))\
.filter(~col("region_id").like("NaN"))

aux_dfs["state"] = aux_dfs["states_and_cities"].select(
    col("Cdigo_da_UF").alias("state_id"),
    col("Nome_da_UF").alias("state_name"),
    col("Sigla_da_UF").alias("state_abrv")
).dropDuplicates()\
.filter(~col("state_id").like("CO_%"))\
.filter(~col("state_id").like("NaN"))

aux_dfs["cities"] = aux_dfs["states_and_cities"].select(
    col("Cdigo_do_Municpio").alias("city_id"),
    col("Nome_do_Municpio").alias("city_name")
).dropDuplicates()\
.filter(~col("city_id").like("CO_%"))\
.filter(~col("city_id").like("NaN"))

aux_dfs["enrollment_stages"] = aux_dfs["enrollment_stages"].select(
    col("Cdigo").alias("enrollment_stage_id"),
    col("Nome_Etapa").alias("enrollment_stage_name")
).dropDuplicates()\
.filter(~col("enrollment_stage_id").like("NaN"))\
.filter(~col("enrollment_stage_name").like("NaN"))

for table in tables:
    dfs[table] = dfs[table].join(aux_dfs["region"], dfs[table]["CO_REGIAO"] == aux_dfs["region"]["region_id"], "left")
    dfs[table] = dfs[table].join(aux_dfs["state"], dfs[table]["CO_UF"] == aux_dfs["state"]["state_id"], "left")
    dfs[table] = dfs[table].join(aux_dfs["cities"], dfs[table]["CO_MUNICIPIO"] == aux_dfs["cities"]["city_id"], "left")
    dfs[table] = dfs[table].join(aux_dfs["enrollment_stages"], dfs[table]["TP_ETAPA_ENSINO"] == aux_dfs["enrollment_stages"]["enrollment_stage_id"], "left")
    
    dfs[table] = dfs[table].withColumn("student_gender", 
                                       when(dfs[table]["TP_SEXO"] == "1","Male")
                                       .when(dfs[table]["TP_SEXO"] == "2","Female")
                                       .otherwise("Other"))
    
    dfs[table] = dfs[table].withColumn("student_ethnicity", 
                                       when(dfs[table]["TP_COR_RACA"] == "1","Branca")
                                       .when(dfs[table]["TP_COR_RACA"] == "2","Preta")
                                       .when(dfs[table]["TP_COR_RACA"] == "3","Parda")
                                       .when(dfs[table]["TP_COR_RACA"] == "4","Amarela")
                                       .when(dfs[table]["TP_COR_RACA"] == "5","Indígena")
                                       .otherwise("Não declarada"))
    
    dfs[table] = dfs[table].select(
        col("ID_ALUNO").alias("student_id"),
        col("ID_MATRICULA").alias("enrollment_id"),
        col("NU_IDADE").alias("student_age"),
        col("student_gender"),
        col("student_ethnicity"),
        col("IN_NECESSIDADE_ESPECIAL").alias("has_special_needs"),
        col("IN_BAIXA_VISAO").alias("is_vision_impaired"),
        col("IN_CEGUEIRA").alias("is_blind"),
        col("IN_DEF_AUDITIVA").alias("is_hearing_impaired"),
        col("IN_DEF_FISICA").alias("is_physically_handicapped"),
        col("IN_DEF_INTELECTUAL").alias("is_intellectual_disabled"),
        col("IN_SURDEZ").alias("is_deaf"),
        col("IN_SURDOCEGUEIRA").alias("is_deaf_blind"),
        col("IN_DEF_MULTIPLA").alias("is_multiple_impaired"),
        col("IN_AUTISMO").alias("is_autist"),
        col("IN_SUPERDOTACAO").alias("is_gifted"),
        col("enrollment_stage_id"),
        col("enrollment_stage_name"),
        col("region_id"),
        col("region_name"),
        col("state_id"),
        col("state_name"),
        col("state_abrv"),
        col("city_id"),
        col("city_name")
    )

union_df = dfs["matricula_co"].union(dfs["matricula_norte"])
union_df = union_df.union(dfs["matricula_nordeste"])
union_df = union_df.union(dfs["matricula_sudeste"])
union_df = union_df.union(dfs["matricula_sul"])

union_df.write.mode("overwrite").parquet(target_path)

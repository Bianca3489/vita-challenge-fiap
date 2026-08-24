"""
glue_bronze_to_silver_sih.py
=============================
AWS Glue Job (PySpark) — camada Bronze -> Silver da fonte SIH/SUS.

Responsabilidades:
  - ler todos os parquet de bronze/sih_sus/
  - tipar colunas corretamente (datas, valores, ids)
  - remover duplicados por N_AIH
  - tratar nulos e outliers grosseiros (ex.: DIAS_PERM negativo)
  - particionar por ano/mes/uf
  - gravar em silver/internacoes/ (parquet, catalogado no Glue Data Catalog)

Como publicar:
  aws s3 cp glue_bronze_to_silver_sih.py s3://<SCRIPTS_BUCKET>/etl/
  aws glue create-job --cli-input-json file://../infra/glue_job_bronze_silver_sih.json
"""
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, IntegerType, DoubleType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATALAKE_BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.ansi.enabled", "false")  # datas em formatos mistos não podem derrubar o job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["DATALAKE_BUCKET"]
SRC = f"s3://{BUCKET}/bronze/sih_sus/"
DST = f"s3://{BUCKET}/silver/internacoes/"

# ---------------------------------------------------------------------------
# LEITURA POR PARTIÇÃO (não tudo de uma vez) — necessário porque os 79 meses
# do histórico foram ingeridos em parte com dado REAL (via PySUS) e em parte
# com dado SINTÉTICO de fallback, e alguns campos vêm com TIPOS DIFERENTES
# entre eles (não só largura numérica diferente — às vezes um campo é texto
# num mês e número puro em outro, ex.: N_AIH). O Spark não sabe decodificar
# esse tipo de conflito numa leitura única, mesmo com schema explícito
# forçado (isso resolve SÓ conflito de largura, tipo int32 vs int64 — não
# resolve texto vs número).
#
# A solução: ler cada pasta de partição (uf=X/ano_mes=Y) SEPARADAMENTE, cada
# uma com seu próprio schema real inferido, converter (.cast) pro tipo final
# desejado (isso sim funciona pra qualquer conversão, inclusive texto<->
# número), e só então juntar tudo com unionByName.
# ---------------------------------------------------------------------------
TIPOS_FINAIS = {
    "N_AIH": StringType(),
    "UF_ZI": StringType(),
    "MUNIC_RES": LongType(),
    "MUNIC_MOV": LongType(),
    "DT_INTER": StringType(),
    "DT_SAIDA": StringType(),
    "DIAG_PRINC": StringType(),
    "PROC_REA": StringType(),
    "VAL_TOT": DoubleType(),
    "DIAS_PERM": LongType(),
    "IDADE": LongType(),
    "SEXO": StringType(),
    "MORTE": LongType(),
    "CNES": LongType(),
}

# lista as pastas de partição reais no S3 (equivalente a "ls -R" só nos
# diretórios, sem baixar arquivo nenhum ainda)
s3_client = boto3.client("s3")
paginator = s3_client.get_paginator("list_objects_v2")
prefixo_base = "bronze/sih_sus/"
pastas_particao = set()
for pagina in paginator.paginate(Bucket=BUCKET, Prefix=prefixo_base):
    for obj in pagina.get("Contents", []):
        # de ".../uf=SP/ano_mes=202001/arquivo.parquet" extrai ".../uf=SP/ano_mes=202001/"
        partes = obj["Key"].split("/")
        pasta = "/".join(partes[:-1])
        if "uf=" in pasta and "ano_mes=" in pasta:
            pastas_particao.add(pasta)

pastas_particao = sorted(pastas_particao)
print(f"[INFO] {len(pastas_particao)} pastas de partição encontradas no Bronze.")

dfs_por_mes = []
for pasta in pastas_particao:
    caminho_completo = f"s3://{BUCKET}/{pasta}/"
    df_mes = spark.read.parquet(caminho_completo)  # SEM schema forçado: schema real de cada mês

    # Como agora lemos pasta por pasta (não a pasta pai), o Spark NÃO infere
    # mais sozinho as colunas de partição (uf, ano_mes) a partir do nome da
    # pasta — isso só acontece quando se lê a partir do diretório pai. Como
    # já sabemos o valor (está no próprio caminho que estamos iterando),
    # extraímos e adicionamos como coluna manualmente.
    valor_uf = next((p.split("=", 1)[1] for p in pasta.split("/") if p.startswith("uf=")), None)
    df_mes = df_mes.withColumn("uf", F.lit(valor_uf))

    for coluna, tipo in TIPOS_FINAIS.items():
        if coluna in df_mes.columns:
            df_mes = df_mes.withColumn(coluna, F.col(coluna).cast(tipo))
        else:
            df_mes = df_mes.withColumn(coluna, F.lit(None).cast(tipo))
    dfs_por_mes.append(df_mes.select(*TIPOS_FINAIS.keys(), "uf"))

df = dfs_por_mes[0]
for df_mes in dfs_por_mes[1:]:
    df = df.unionByName(df_mes, allowMissingColumns=True)


def parse_data_sih(col_name: str):
    """O SIH/SUS real mistura formatos de data entre os campos (ex.: DT_INTER
    vem como 'yyyy-MM-dd' mas DT_SAIDA vem como 'yyyyMMdd', 8 dígitos sem
    separador). Esta função detecta o formato pelo padrão do texto ANTES de
    converter, então nunca lança erro — datas em formato desconhecido viram
    NULL em vez de derrubar o job inteiro."""
    c = F.col(col_name).cast("string")
    return (
        F.when(c.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(c, "yyyy-MM-dd"))
        .when(c.rlike(r"^\d{8}$"), F.to_date(c, "yyyyMMdd"))
        .otherwise(F.lit(None).cast("date"))
    )


df_clean = (
    df
    .dropDuplicates(["N_AIH"])
    .withColumn("VAL_TOT", F.col("VAL_TOT").cast(DoubleType()))
    .withColumn("DIAS_PERM", F.col("DIAS_PERM").cast(IntegerType()))
    .withColumn("IDADE", F.col("IDADE").cast(IntegerType()))
    .withColumn("DT_INTER", parse_data_sih("DT_INTER"))
    .withColumn("DT_SAIDA", parse_data_sih("DT_SAIDA"))
    # regras de qualidade mínimas
    .filter(F.col("DIAS_PERM") >= 0)
    .filter(F.col("VAL_TOT") >= 0)
    .filter(F.col("DT_INTER").isNotNull())
    # colunas derivadas usadas no Gold
    .withColumn("ano", F.year("DT_INTER"))
    .withColumn("mes", F.month("DT_INTER"))
    .withColumn("semana_epidemiologica", F.weekofyear("DT_INTER"))
    .withColumnRenamed("CNES", "cnes_id")
    .withColumnRenamed("DIAG_PRINC", "cid_principal")
    .withColumnRenamed("MUNIC_MOV", "municipio_id")
    .withColumnRenamed("VAL_TOT", "valor_total")
    .withColumnRenamed("DIAS_PERM", "dias_permanencia")
    # NÃO renomear UF_ZI -> uf: o Spark já cria uma coluna "uf" sozinho, lida
    # automaticamente da estrutura de pastas Hive-style (bronze/sih_sus/uf=SP/...)
    # que a ingestão grava. Ter as duas com o mesmo nome causa
    # "AnalysisException: Reference 'uf' is ambiguous". Mantemos UF_ZI como
    # referência (UF de residência do paciente, que pode diferir da UF do
    # hospital em casos de atendimento fora do estado), só renomeada pra
    # deixar claro que não é a partição.
    .withColumnRenamed("UF_ZI", "uf_residencia_paciente")
)

(
    df_clean
    .repartition("ano", "mes", "uf")
    .write
    .mode("overwrite")
    .partitionBy("ano", "mes", "uf")
    .parquet(DST)
)

# Atualiza o catálogo (equivalente a rodar um crawler, mas determinístico)
glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [DST], "recurse": True},
    format="parquet",
).toDF().createOrReplaceTempView("silver_internacoes_preview")

job.commit()

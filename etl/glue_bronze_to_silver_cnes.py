"""
glue_bronze_to_silver_cnes.py
===============================
AWS Glue Job (PySpark) — camada Bronze -> Silver da fonte CNES (JSON).

⚠️ Reescrito com o SCHEMA REAL da API (confirmado testando com dado de
verdade — a documentação informal do portal está desatualizada). Campos como
usados de fato pela API:
  codigo_cnes, nome_fantasia, codigo_uf (código IBGE, ex.: 35=SP),
  codigo_municipio, codigo_tipo_unidade (numérico),
  latitude_estabelecimento_decimo_grau, longitude_estabelecimento_decimo_grau,
  estabelecimento_possui_atendimento_hospitalar (0/1), data_atualizacao.

NÃO existe leitos_totais/leitos_sus nesse endpoint (é um endpoint de
CADASTRO, não de capacidade). Por isso essas colunas ficam nulas aqui — se
mais pra frente vocês acharem/usarem o endpoint de leitos do CNES, dá pra
enriquecer essa camada depois. Por ora, o índice de pressão hospitalar
(camada Gold) precisa saber lidar com leitos_sus nulo (estimar de outra
forma ou usar só as variáveis disponíveis).

Responsabilidades:
  - ler o JSON Lines de bronze/cnes/
  - renomear/tipar os campos reais para os nomes usados no resto do pipeline
  - converter codigo_uf (numérico IBGE) -> uf (sigla, ex.: 35 -> 'SP'), para
    bater com o campo `uf` usado no SIH (fonte 1) e no CSV auxiliar (fonte 3)
  - derivar uma classificação simples de tipo de unidade (hospitalar ou não)
    a partir de codigo_tipo_unidade, só como referência — o filtro definitivo
    de quais tipos usar fica pra camada Gold
  - deduplicar por codigo_cnes (fica sempre com o registro mais recente)
  - gravar em silver/estabelecimentos/ particionado por uf
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATALAKE_BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.ansi.enabled", "false")
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["DATALAKE_BUCKET"]
SRC = f"s3://{BUCKET}/bronze/cnes/"
DST = f"s3://{BUCKET}/silver/estabelecimentos/"

df = spark.read.json(SRC)  # schema inferido automaticamente do JSON Lines

# --- Código IBGE (2 dígitos) -> sigla de UF -----------------------------
# Mesma tabela usada na ingestão (ingestion/ingest_cnes_api.py), só invertida.
CODIGO_IBGE_PARA_UF = {
    12: "AC", 27: "AL", 16: "AP", 13: "AM", 29: "BA", 23: "CE", 53: "DF",
    32: "ES", 52: "GO", 21: "MA", 51: "MT", 50: "MS", 31: "MG", 15: "PA",
    25: "PB", 41: "PR", 26: "PE", 22: "PI", 33: "RJ", 24: "RN", 43: "RS",
    11: "RO", 14: "RR", 42: "SC", 35: "SP", 28: "SE", 17: "TO",
}
mapa_uf = F.create_map([F.lit(x) for kv in CODIGO_IBGE_PARA_UF.items() for x in kv])

# --- código_tipo_unidade -> classificação simples (referência) ---------
# Tabela de domínio do CNES (subconjunto mais comum). O filtro definitivo de
# quais tipos entram na análise de capacidade hospitalar fica pra Gold.
TIPOS_HOSPITALARES = {5, 7, 15, 20, 21, 62}  # Hospital Geral/Especializado/
                                              # Unidade Mista/Pronto Socorro/
                                              # Hospital-Dia

df_typed = (
    df
    .withColumn("codigo_uf", F.col("codigo_uf").cast(IntegerType()))
    .withColumn("uf", mapa_uf[F.col("codigo_uf")])
    .withColumn("codigo_municipio", F.col("codigo_municipio").cast(IntegerType()))
    .withColumn("codigo_tipo_unidade", F.col("codigo_tipo_unidade").cast(IntegerType()))
    .withColumn(
        "latitude",
        F.col("latitude_estabelecimento_decimo_grau").cast(DoubleType()),
    )
    .withColumn(
        "longitude",
        F.col("longitude_estabelecimento_decimo_grau").cast(DoubleType()),
    )
    .withColumn(
        "possui_atendimento_hospitalar",
        F.col("estabelecimento_possui_atendimento_hospitalar").cast(IntegerType()),
    )
    .withColumn(
        "eh_tipo_hospitalar",
        F.col("codigo_tipo_unidade").isin(list(TIPOS_HOSPITALARES)),
    )
    # leitos_totais/leitos_sus NÃO existem nesse endpoint — ficam nulos por
    # enquanto; a Gold precisa tolerar isso (ver etl/glue_silver_to_gold_kpis.py)
    .withColumn("leitos_totais", F.lit(None).cast(IntegerType()))
    .withColumn("leitos_sus", F.lit(None).cast(IntegerType()))
    # data_atualizacao real vem como 'yyyy-MM-dd' (texto simples, sem hora)
    .withColumn("data_atualizacao", F.to_date(F.col("data_atualizacao"), "yyyy-MM-dd"))
    .withColumnRenamed("nome_fantasia", "nome_estabelecimento")
)

# mantém somente o registro mais recente por hospital (SCD tipo 1 simplificado)
w = Window.partitionBy("codigo_cnes").orderBy(F.col("data_atualizacao").desc_nulls_last())
df_dedup = (
    df_typed
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumnRenamed("codigo_cnes", "cnes_id")
    .filter(F.col("uf").isNotNull())  # descarta o que não deu pra mapear a UF
    .select(
        "cnes_id", "nome_estabelecimento", "codigo_tipo_unidade", "eh_tipo_hospitalar",
        "uf", "codigo_municipio", "latitude", "longitude",
        "leitos_totais", "leitos_sus", "possui_atendimento_hospitalar",
        "data_atualizacao",
    )
)

(
    df_dedup
    .write
    .mode("overwrite")
    .partitionBy("uf")
    .parquet(DST)
)

job.commit()

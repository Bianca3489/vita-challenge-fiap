"""
glue_silver_to_gold_kpis.py
=============================
AWS Glue Job (PySpark + scikit-learn) — camada Silver -> Gold.

Monta o modelo dimensional (fato_internacoes, dim_hospital, dim_tempo) e as
tabelas analíticas (kpi_pressao_hospitalar, kpi_tendencia_doencas), incluindo
o Índice de Pressão Hospitalar (K-Means + regras), que é o "diferencial
inovador" apresentado no pitch (slide "Índice Inteligente de Pressão
Hospitalar").

⚠️ AJUSTE IMPORTANTE (dado real): o endpoint do CNES que usamos é de
CADASTRO, não traz `leitos_sus`/`leitos_totais` (ficam NULL desde a camada
Silver). Por isso o índice de pressão AQUI é calculado com base em
volume de internações, permanência média e pacientes críticos —
comparados relativamente entre hospitais do mesmo período (não como uma
"% de ocupação" absoluta, que exigiria saber a capacidade de leitos).
Se no futuro vocês conseguirem uma fonte real de leitos (ex.: outro
endpoint do CNES ou o DataSUS "Hospitais e Leitos"), dá pra somar esse
componente na fórmula sem redesenhar o resto.

Dependências extra no Glue Job: scikit-learn, pandas (ver
--additional-python-modules em infra/glue_job_silver_gold.json)
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATALAKE_BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.ansi.enabled", "false")
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["DATALAKE_BUCKET"]
SILVER_INTERNACOES = f"s3://{BUCKET}/silver/internacoes/"
SILVER_ESTAB = f"s3://{BUCKET}/silver/estabelecimentos/"
GOLD = f"s3://{BUCKET}/gold/"

internacoes = spark.read.parquet(SILVER_INTERNACOES)
estabelecimentos = spark.read.parquet(SILVER_ESTAB)

# ---------------------------------------------------------------------------
# 1) DIM_HOSPITAL (schema real do CNES — sem nome de município, só código;
#    quem quiser o nome do município faz join com municipios_referencia no
#    Athena, ver analytics/athena_gold_views.sql)
# ---------------------------------------------------------------------------
dim_hospital = estabelecimentos.select(
    "cnes_id", "nome_estabelecimento", "codigo_tipo_unidade", "eh_tipo_hospitalar",
    "uf", "codigo_municipio", "latitude", "longitude",
    "leitos_totais", "leitos_sus", "possui_atendimento_hospitalar",
)

dim_hospital.write.mode("overwrite").parquet(f"{GOLD}dim_hospital/")

# ---------------------------------------------------------------------------
# 2) DIM_TEMPO
# ---------------------------------------------------------------------------
dim_tempo = (
    internacoes
    .select("ano", "mes", "semana_epidemiologica")
    .distinct()
    .withColumn("trimestre", ((F.col("mes") - 1) / 3 + 1).cast("int"))
)
dim_tempo.write.mode("overwrite").parquet(f"{GOLD}dim_tempo/")

# ---------------------------------------------------------------------------
# 3) FATO_INTERNACOES
# ---------------------------------------------------------------------------
fato_internacoes = internacoes.select(
    "cnes_id", "municipio_id", "ano", "mes", "semana_epidemiologica",
    "cid_principal", "valor_total", "dias_permanencia",
)
fato_internacoes.write.mode("overwrite").partitionBy("ano", "mes").parquet(
    f"{GOLD}fato_internacoes/"
)

# ---------------------------------------------------------------------------
# 4) KPI_TENDENCIA_DOENCAS (uf x doença x semana, variação vs. semana anterior)
#    (agrupa só por UF, não por município — o dado de internação não carrega
#    o nome do município, só o código; se quiser por município, faça o join
#    com municipios_referencia direto no Athena)
# ---------------------------------------------------------------------------
casos_semana = (
    fato_internacoes
    .join(dim_hospital.select("cnes_id", "uf"), "cnes_id", "left")
    .groupBy("uf", "cid_principal", "ano", "mes", "semana_epidemiologica")
    .agg(F.count("*").alias("qtd_casos"))
)

from pyspark.sql.window import Window  # noqa: E402 (import local pra clareza)

w_lag = Window.partitionBy("uf", "cid_principal").orderBy("ano", "mes", "semana_epidemiologica")
kpi_tendencia = (
    casos_semana
    .withColumn("qtd_casos_semana_anterior", F.lag("qtd_casos").over(w_lag))
    .withColumn(
        "variacao_pct",
        F.when(F.col("qtd_casos_semana_anterior") > 0,
               (F.col("qtd_casos") - F.col("qtd_casos_semana_anterior"))
               / F.col("qtd_casos_semana_anterior") * 100)
        .otherwise(F.lit(None).cast(DoubleType())),
    )
)
kpi_tendencia.write.mode("overwrite").parquet(f"{GOLD}kpi_tendencia_doencas/")

# ---------------------------------------------------------------------------
# 5) ÍNDICE DE PRESSÃO HOSPITALAR (K-Means + regras) — granularidade hospital
#    SEM depender de leitos_sus (indisponível na fonte real). Usa 3 sinais
#    que TEMOS de verdade: volume de internações, permanência média e
#    pacientes críticos (internação >= 10 dias) — comparados relativamente
#    entre os hospitais do mesmo ano/mês.
# ---------------------------------------------------------------------------
features_spark = (
    fato_internacoes
    .groupBy("cnes_id", "ano", "mes")
    .agg(
        F.count("*").alias("qtd_internacoes"),
        F.avg("dias_permanencia").alias("permanencia_media"),
        F.sum(F.when(F.col("dias_permanencia") >= 10, 1).otherwise(0)).alias("pacientes_criticos"),
    )
    # NÃO junta mais com leitos_sus: essa coluna é sempre nula (a fonte real
    # do CNES não traz contagem de leitos nesse endpoint) e uma coluna
    # 100% nula do tipo inteiro vira "double" quando passa pelo pandas
    # (toPandas/to_dict), o que quebra a leitura no Athena
    # ("HIVE_BAD_DATA: ... incompatible with type integer"). Como o índice
    # de pressão já não depende dela, é mais simples remover do que corrigir
    # o tipo toda vez.
)

pdf = features_spark.toPandas()

if not pdf.empty:
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler

    feature_cols = ["qtd_internacoes", "permanencia_media", "pacientes_criticos"]
    pdf[feature_cols] = pdf[feature_cols].fillna(0)

    X = StandardScaler().fit_transform(pdf[feature_cols])
    k = min(4, max(1, pdf.shape[0]))  # nunca pedir mais clusters que linhas
    kmeans = KMeans(n_clusters=k, n_init=10, random_state=42)
    pdf["cluster"] = kmeans.fit_predict(X)

    # ordena os clusters pelo centro médio de qtd_internacoes -> risco crescente
    # (mais internações concentradas relativamente = mais pressão)
    centro_por_cluster = pdf.groupby("cluster")["qtd_internacoes"].mean().sort_values()
    ordem_risco = {cluster: rank for rank, cluster in enumerate(centro_por_cluster.index)}
    niveis = ["Baixo risco", "Atenção", "Crítico", "Sobrecarga iminente"]
    pdf["nivel_risco"] = pdf["cluster"].map(ordem_risco).map(
        lambda r: niveis[min(r, len(niveis) - 1)]
    )

    # índice 0-100: normaliza cada sinal pelo máximo do próprio período
    # (ano/mes), assim compara hospitais entre si, não contra um valor fixo
    max_internacoes = pdf["qtd_internacoes"].max() or 1
    max_permanencia = pdf["permanencia_media"].max() or 1
    max_criticos = pdf["pacientes_criticos"].max() or 1

    pdf["indice_pressao"] = (
        (pdf["qtd_internacoes"] / max_internacoes) * 50
        + (pdf["permanencia_media"] / max_permanencia) * 30
        + (pdf["pacientes_criticos"] / max_criticos) * 20
    ).round(1)

    # Usar to_dict('records') em vez de passar o pandas.DataFrame direto:
    # o PySpark do Glue 4.0 tem uma rotina interna de conversão pandas->Spark
    # que ainda chama pdf.iteritems() — método removido no pandas 2.0
    # ("AttributeError: 'DataFrame' object has no attribute 'iteritems'").
    # Convertendo via lista de dicionários, esse caminho problemático nem é
    # usado — funciona independente da versão do pandas.
    kpi_pressao_spark = spark.createDataFrame(pdf.to_dict("records"))
else:
    kpi_pressao_spark = features_spark.withColumn("cluster", F.lit(None)) \
        .withColumn("nivel_risco", F.lit(None)) \
        .withColumn("indice_pressao", F.lit(None))

kpi_pressao_spark.write.mode("overwrite").parquet(f"{GOLD}kpi_pressao_hospitalar/")

job.commit()

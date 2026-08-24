-- =============================================================================
-- athena_ddl.sql
-- Cria as tabelas externas no Glue Data Catalog / Amazon Athena.
-- Rode em ordem, ajustando <DATALAKE_BUCKET> para o bucket real.
-- =============================================================================

-- --------------------------------------------------------------------------
-- Databases
-- --------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS vita_gold;

-- --------------------------------------------------------------------------
-- FONTE 3 (CSV Auxiliar) — External Table, exatamente como pedido no edital
-- ("lido diretamente como tabela dentro do banco, sem carga manual")
-- --------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS vita_gold.municipios_referencia (
    codigo_municipio     BIGINT,
    nome_municipio       STRING,
    uf                   STRING,
    regiao_saude         STRING,
    populacao            BIGINT,
    meta_leitos_1000hab  DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://<DATALAKE_BUCKET>/bronze/csv_auxiliar/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- --------------------------------------------------------------------------
-- GOLD — fato e dimensões (particionadas; rode MSCK REPAIR TABLE após novas
-- partições ou use Glue Crawler agendado)
-- --------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS vita_gold.fato_internacoes (
    cnes_id                BIGINT,
    municipio_id           BIGINT,
    semana_epidemiologica  INT,
    cid_principal           STRING,
    valor_total             DOUBLE,
    dias_permanencia        INT
)
PARTITIONED BY (ano INT, mes INT)
STORED AS PARQUET
LOCATION 's3://<DATALAKE_BUCKET>/gold/fato_internacoes/';

MSCK REPAIR TABLE vita_gold.fato_internacoes;

CREATE EXTERNAL TABLE IF NOT EXISTS vita_gold.dim_hospital (
    cnes_id                       BIGINT,
    nome_estabelecimento          STRING,
    codigo_tipo_unidade           INT,
    eh_tipo_hospitalar            BOOLEAN,
    uf                            STRING,
    codigo_municipio              BIGINT,
    latitude                      DOUBLE,
    longitude                     DOUBLE,
    leitos_totais                 INT,
    leitos_sus                    INT,
    possui_atendimento_hospitalar INT
)
STORED AS PARQUET
LOCATION 's3://<DATALAKE_BUCKET>/gold/dim_hospital/';

CREATE EXTERNAL TABLE IF NOT EXISTS vita_gold.dim_tempo (
    ano                     INT,
    mes                     INT,
    semana_epidemiologica   INT,
    trimestre               INT
)
STORED AS PARQUET
LOCATION 's3://<DATALAKE_BUCKET>/gold/dim_tempo/';

CREATE EXTERNAL TABLE IF NOT EXISTS vita_gold.kpi_pressao_hospitalar (
    cnes_id                  BIGINT,
    ano                      INT,
    mes                      INT,
    qtd_internacoes          BIGINT,
    permanencia_media        DOUBLE,
    pacientes_criticos       BIGINT,
    cluster                  INT,
    nivel_risco              STRING,
    indice_pressao           DOUBLE
)
STORED AS PARQUET
LOCATION 's3://<DATALAKE_BUCKET>/gold/kpi_pressao_hospitalar/';

CREATE EXTERNAL TABLE IF NOT EXISTS vita_gold.kpi_tendencia_doencas (
    uf                          STRING,
    cid_principal                STRING,
    ano                          INT,
    mes                          INT,
    semana_epidemiologica        INT,
    qtd_casos                    BIGINT,
    qtd_casos_semana_anterior    BIGINT,
    variacao_pct                 DOUBLE
)
STORED AS PARQUET
LOCATION 's3://<DATALAKE_BUCKET>/gold/kpi_tendencia_doencas/';

-- =============================================================================
-- athena_gold_views.sql
-- Views de negócio consumidas diretamente pelo Power BI — cada uma corresponde
-- a uma tela do protótipo (Dashboard Executivo / Visão Evolutiva / Mapa / Alertas).
--
-- IMPORTANTE: dim_hospital NÃO tem nome de município como texto (a fonte real
-- do CNES só traz codigo_municipio). O nome do município vem via JOIN com
-- municipios_referencia (Fonte 3 — CSV). Municípios que não estiverem
-- cadastrados nessa tabela de referência aparecem com nome nulo (é esperado,
-- a tabela de referência é só uma amostra de municípios de exemplo).
-- =============================================================================

-- Tela "Acompanhamento Geral": internações, pressão, risco de colapso
CREATE OR REPLACE VIEW vita_gold.vw_acompanhamento_geral AS
SELECT
    f.ano,
    f.mes,
    h.uf,
    m.nome_municipio,
    COUNT(*)                    AS qtd_internacoes,
    SUM(f.valor_total)          AS valor_total_pago,
    AVG(f.dias_permanencia)     AS permanencia_media,
    AVG(p.indice_pressao)       AS indice_pressao_medio,
    MAX(p.nivel_risco)          AS nivel_risco_predominante
FROM vita_gold.fato_internacoes f
JOIN vita_gold.dim_hospital h            ON h.cnes_id = f.cnes_id
LEFT JOIN vita_gold.municipios_referencia m ON m.codigo_municipio = h.codigo_municipio
LEFT JOIN vita_gold.kpi_pressao_hospitalar p
       ON p.cnes_id = f.cnes_id AND p.ano = f.ano AND p.mes = f.mes
GROUP BY f.ano, f.mes, h.uf, m.nome_municipio;

-- Tela "Visão Evolutiva": evolução mensal por doença (para o gráfico de linhas)
CREATE OR REPLACE VIEW vita_gold.vw_tendencia_doencas AS
SELECT
    uf,
    cid_principal,
    ano,
    mes,
    SUM(qtd_casos)          AS qtd_casos_mes,
    AVG(variacao_pct)       AS variacao_media_pct
FROM vita_gold.kpi_tendencia_doencas
GROUP BY uf, cid_principal, ano, mes;

-- Tela "Mapa de Pressão por Região": para o mapa de calor do Power BI
CREATE OR REPLACE VIEW vita_gold.vw_pressao_por_regiao AS
SELECT
    h.uf,
    m.nome_municipio,
    h.latitude,
    h.longitude,
    m.regiao_saude,
    AVG(p.indice_pressao)                                            AS indice_pressao_medio,
    ARRAY_AGG(DISTINCT p.nivel_risco)[1]                             AS nivel_risco_regiao,
    CAST(SUM(f.dias_permanencia) AS DOUBLE) / NULLIF(m.populacao, 0) * 1000 AS internacoes_por_1000hab
FROM vita_gold.kpi_pressao_hospitalar p
JOIN vita_gold.dim_hospital h ON h.cnes_id = p.cnes_id
JOIN vita_gold.fato_internacoes f ON f.cnes_id = p.cnes_id AND f.ano = p.ano AND f.mes = p.mes
LEFT JOIN vita_gold.municipios_referencia m ON m.codigo_municipio = h.codigo_municipio
GROUP BY h.uf, m.nome_municipio, h.latitude, h.longitude, m.regiao_saude, m.populacao;

-- Tela "Alertas": ranking de hospitais críticos (top N por indice_pressao)
CREATE OR REPLACE VIEW vita_gold.vw_ranking_hospitais_criticos AS
SELECT
    h.nome_estabelecimento,
    h.uf,
    m.nome_municipio,
    p.ano,
    p.mes,
    p.indice_pressao,
    p.nivel_risco,
    p.qtd_internacoes,
    p.permanencia_media,
    RANK() OVER (PARTITION BY p.ano, p.mes ORDER BY p.indice_pressao DESC) AS ranking_criticidade
FROM vita_gold.kpi_pressao_hospitalar p
JOIN vita_gold.dim_hospital h ON h.cnes_id = p.cnes_id
LEFT JOIN vita_gold.municipios_referencia m ON m.codigo_municipio = h.codigo_municipio
WHERE p.nivel_risco IN ('Crítico', 'Sobrecarga iminente');

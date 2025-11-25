-- ==============================
-- SCHEMAS
-- ==============================
CREATE SCHEMA IF NOT EXISTS oni_dev.ambiente_teste;

-- ==============================
-- TABLES & VIEWS (ambiente_teste)
-- ==============================
CREATE TABLE IF NOT EXISTS oni_dev.ambiente_teste.tabela_de_teste AS
SELECT * FROM datalake__biz.oni.oni_bases_referencia__uf;

CREATE OR REPLACE VIEW oni_dev.ambiente_teste.tabela_de_teste AS
SELECT * FROM datalake__biz.oni.oni_bases_referencia__uf;

CREATE TEMPORARY VIEW minha_view_temp AS
SELECT * FROM datalake__biz.oni.oni_bases_referencia__uf;

CREATE OR REPLACE TEMPORARY VIEW minha_view_temp AS
SELECT * FROM datalake__biz.oni.oni_bases_referencia__uf;

SELECT * FROM minha_view_temp;
SELECT * FROM minha_view_temp;

DROP TABLE IF EXISTS oni_dev.ambiente_teste.tabela_de_teste;

SHOW VIEWS;

-- ==============================
-- TABLES & VIEWS (obs_amazonia21)
-- ==============================
CREATE OR REPLACE TABLE oni_rede_colaborativa.obs_amazonia21.tabela_de_teste
USING PARQUET
LOCATION '/Volumes/oni_rede_colaborativa/obs_amazonia21/tabela_de_teste'
AS
SELECT * 
FROM datalake__biz.oni.oni_bases_referencia__uf;

CREATE OR REPLACE VIEW oni_rede_colaborativa.obs_amazonia21.tabela_de_teste AS
SELECT * FROM datalake__biz.oni.oni_bases_referencia__uf;

-- ==============================
-- ALTER TABLE / ALTER VIEW
-- ==============================
ALTER TABLE oni_dev.ambiente_teste.tabela_de_teste
SET TBLPROPERTIES (
    'classification' = 'confidential',
    'data_owner' = 'finance-team',
    'pii' = 'true'
);

ALTER VIEW oni_dev.ambiente_teste.tabela_de_teste
SET TBLPROPERTIES (
    'classification' = 'confidential',
    'data_owner' = 'finance-team',
    'pii' = 'true'
);

ALTER TABLE oni_dev.ambiente_teste.tabela_de_teste
oni_data_hub.catalogo_de_dados.vw_catalogo_biz ('classification' = 'confidential');

ALTER TABLE oni_dev.ambiente_teste.tabela_de_teste
UNSET TAGS ('classification', 'pii');

-- ==============================
-- QUERIES
-- ==============================
SELECT * FROM oni_dev.ambiente_teste.tabela_de_teste;

SELECT * FROM oni_data_hub.catalogo_de_dados.vw_catalogo_raw;

SELECT * FROM DELTA.`/Volumes/oni_dev/default/tmp_dev_raw_usr_oni/midr/fco/carteira/`;

SELECT * FROM oni_mte_novo_caged__identificada
WHERE CD_ANO_MES_COMPETENCIA = 202506;

SELECT * FROM datalake__trs.oni.oni_mte_novo_caged__identificada;

SELECT ANO,
  COUNT(*) AS contagem
FROM oni_mte_novo_caged__identificada
WHERE COMPETENCIAMOV = 202506
GROUP BY ANO
ORDER BY ANO ASC;

SELECT * FROM datalake__trs.oni.oni_mte_novo_caged__identificada
WHERE COMPETENCIAMOV = 202506;

SELECT * FROM datalake__trs.oni.oni_mte_novo_caged__identificada;

SELECT ANO,
  COUNT(*) AS contagem
FROM oni_mte_novo_caged__identificada
WHERE COMPETENCIAMOV = 202506
GROUP BY ANO
ORDER BY ANO ASC;

-- ==============================
-- FILES / PATHS (referências)
-- ==============================
datalake__trs.oni.oni_mte_novo_caged__identificada;
datalake__trs.oni.oni_mte_novo_caged__identificada;
/Volumes/oni_rede_colaborativa/obs_amazonia21/uds_oni_rede_colaborativa_observatorio_amazonia21_dados_consulta;

-- ==============================
-- CSV INGEST (municipios PNDR)
-- ==============================
CREATE OR REPLACE TABLE oni_lab.default.municipios_pndr
USING CSV
OPTIONS (
  header "true",
  delimiter ";",
  encoding "UTF-8"
)
LOCATION '/Volumes/oni_lab/default/uds_oni_observatorio_nacional/rel_municipios_portarian_nova_tipologia_PNDR/RelatorioMunicipiosPortaria34de18.01.2018NovaTipologiaPNDR.csv';

CREATE OR REPLACE TEMPORARY VIEW vw_municipios
USING CSV
OPTIONS (
  header "true",
  delimiter ";",
  encoding "ISO-8859-1"
)
LOCATION '/Volumes/oni_lab/default/uds_oni_observatorio_nacional/rel_municipios_portarian_nova_tipologia_PNDR/RelatorioMunicipiosPortaria34de18.01.2018NovaTipologiaPNDR.csv';

-- Erro ao tentar rodar com LOCATION em TEMP VIEW
[PARSE_SYNTAX_ERROR] Syntax error at or near 'LOCATION'. SQLSTATE: 42601 line 8, pos 0

-- Alternativa usando csv.`path`
SELECT *
FROM csv.`/Volumes/oni_lab/default/uds_oni_observatorio_nacional/rel_municipios_portarian_nova_tipologia_PNDR/RelaodoMunicipiosPortarian34de18.01.2018NovaTipologiaPNDR.csv`
WITH (
  header = "true",
  delimiter = ";",
  encoding = "ISO-8859-1"
);

-- ==============================
-- OUTRAS CONSULTAS
-- ==============================
SELECT * FROM oni_abstartups_mapeamento_lista__startups;

SELECT * FROM datalake__trs.oni.oni_midr_fno__desembolsos;

`/Volumes/oni_rede_colaborativa/obs_amazonia21/uds_oni_rede_colaborativa_observatorio_amazonia21_dados_consulta`;

-- ==============================
-- VIEW vw_catalogo_raw
-- ==============================
SELECT * FROM vw_catalogo_raw WITH raw AS (
  SELECT
    table_catalog,
    table_name,
    comment
  FROM datalake__raw_crw.information_schema.tables
  WHERE table_schema = 'oni'
  UNION ALL
  SELECT
    table_catalog,
    table_name,
    comment
  FROM datalake__raw_usr.information_schema.tables
  WHERE table_schema = 'oni'
  ORDER BY table_name
),
tags AS (
  SELECT
    table_name,
    catalog_name,
    tag_name,
    tag_value
  FROM datalake__raw_crw.information_schema.table_tags
  WHERE schema_name = 'oni'
  UNION ALL
  SELECT
    table_name,
    catalog_name,
    tag_name,
    tag_value
  FROM datalake__raw_usr.information_schema.table_tags
  WHERE schema_name = 'oni'
  ORDER BY table_name
),
catalogo_pivot AS (
  SELECT *
  FROM (
    SELECT
      r.*,
      t.tag_name,
      t.tag_value
    FROM raw r
      LEFT JOIN tags t
        ON r.table_name = t.table_name
        AND r.table_catalog = t.catalog_name
  ) t
  PIVOT (
    MAX(tag_value) FOR tag_name IN (
      'fonte',
      'base_de_dados',
      'eixo',
      'tipo_de_base',
      'tipo_tabela',
      'série_histórica',
      'tipo_ingestão',
      'periodicidade',
      'data_steward',
      'produtos_associados',
      'url_origem'
    )
  )
)
SELECT *
FROM catalogo_pivot
WHERE table_name NOT IN (
  'fiesc_bacen_expec_mercado__trimestral',
  'fiesc_ibge_inpc__7063',
  'fiesc_ibge_pms__is_sidra_pms',
  'fiesc_ibge_sidra__ipca',
  'fiesc_ibge_sidra__pim_pf',
  'fiesc_me_comex_export__ncm',
  'fiesc_me_comex_export__mun',
  'fiesc_mec_inep_censo_es__cursos',
  'fiesc_mec_inep_censo_es__ies',
  'fiesc_sidra__1849_fato'
);
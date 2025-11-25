    WITH json_validos AS (
        SELECT *
        FROM sngb.local_armazenamento_estrutura
        WHERE estrutura_armazenamento_json ~ '^\{.*\}$'
    ),
    json_invalidos AS (
        SELECT *
        FROM sngb.local_armazenamento_estrutura
        WHERE estrutura_armazenamento_json !~ '^\{.*\}$' OR estrutura_armazenamento_json IS NULL
    ),
    json_expandido AS (
        SELECT
            lae.*,  
            lae.estrutura_armazenamento_json::json ->> 'bairro' AS bairro,
            lae.estrutura_armazenamento_json::json ->> 'cep' AS cep,
            lae.estrutura_armazenamento_json::json ->> 'complemento' AS complemento,        
            lae.estrutura_armazenamento_json::json ->> 'idEndereco' AS idEndereco,
            lae.estrutura_armazenamento_json::json ->> 'idEstado' AS idEstado,
            lae.estrutura_armazenamento_json::json ->> 'idMunicipio' AS idMunicipio,
            lae.estrutura_armazenamento_json::json ->> 'idPais' AS idPais,
            lae.estrutura_armazenamento_json::json ->> 'logradouro' AS logradouro,        
            lae.estrutura_armazenamento_json::json ->> 'municipio' AS municipio,        
            lae.estrutura_armazenamento_json::json ->> 'localArmazenamentoId' AS localArmazenamentoId,
            lae.estrutura_armazenamento_json::json ->> 'numero' AS numero,
            lae.estrutura_armazenamento_json::json ->> 'pais' AS pais
        FROM json_validos AS lae
    ),
    json_completo AS (
        SELECT e.* FROM json_expandido e
        UNION ALL
        SELECT
            i.*,
            NULL AS bairro,
            NULL AS cep,
            NULL AS complemento,
            NULL AS idEndereco,
            NULL AS idEstado,
            NULL AS idMunicipio,
            NULL AS idPais,
            NULL AS logradouro,
            NULL AS municipio,
            NULL AS localArmazenamentoId,
            NULL AS numero,
            NULL AS pais
        FROM json_invalidos AS i
    )
    SELECT 
        json_build_object(
            'localArmazenamento', json_build_object(
                'idSngb', local_armazenamento.id::text,
                'nome', local_armazenamento.nome,
                'tipo', json_build_object(
                    'idSngb', tipo_local_armazenamento.id::text,
                    'codigo', tipo_local_armazenamento.codigo,
                    'nome', tipo_local_armazenamento.nome
                ),
                'endereço', json_build_object(
                    'idSngb', json_completo.id::text,
                    'idEndereco', json_completo.idEndereco,
                    'logradouro', json_completo.logradouro,
                    'numero', json_completo.numero,
                    'complemento', json_completo.complemento,
                    'bairro', json_completo.bairro,
                    'cep', json_completo.cep,
                    'pais_nome', 'Brasil',
                    'localidade', json_build_object(
                        'codigo', json_completo.idMunicipio,
                        'descricao', json_completo.municipio
                    ),
                    'uf', json_build_object(
                        'idSngb', json_completo.idEstado::text,
                        'sigla', ibge_uf.sigla_uf,
                        'nome', ibge_uf.nome_uf
                    ),
                    'pais', json_build_object(
                        'idSngb', '26',
                        'sigla', 'BR',
                        'nome', 'Brasil'
                    ),
                    'excluido', false
                ),
                'excluido', false
            )
        ) AS local_armazenamento_json
    FROM json_completo
    LEFT JOIN sngb.local_armazenamento  
        ON CAST(json_completo.localArmazenamentoId AS BIGINT) = local_armazenamento.id
    LEFT JOIN sngb.tipo_local_armazenamento
        ON local_armazenamento.tipo_local_armazenamento_id = tipo_local_armazenamento.id   
    LEFT JOIN sngb.situacao AS situacao_acesso
        ON situacao_acesso.id = local_armazenamento.situacao_id    
    LEFT JOIN sngb.ibge_uf
        ON ibge_uf.codigo_uf_ibge = CAST(json_completo.idEstado AS INTEGER)
    LEFT JOIN sngb.ibge_municipios
        ON ibge_municipios.codigo_municipio_ibge = CAST(json_completo.idMunicipio AS INTEGER)
    WHERE local_armazenamento.codigo_orgao =  1334;
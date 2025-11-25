SELECT DISTINCT
	b.id as json_id,
	b.ctr_dth_inc as json_dataHoraInclusaoSngb,
	b.ctr_dth_atu as json_dataHoraAtualizacaoSngb,
	b.data_restricao as json_data_restricao,
	be.id as json_especificacao_id,
	be.nome_atributo as json_especificacao_nome,
	b.texto_especificacao_bem as json_especificacao_texto,
	b.observacoes as json_observacoes,
	s.id as json_situacao_situacao_id,
	s.nome_situacao as json_situacao_nome,
	c.id as json_categoria_id,
	c.codigo_categoria as json_categoria_codigo,
	c.nome_categoria as json_categoria_nome,
	cla.id as json_classe_id,
	cla.codigo_classe as json_classe_codigo,
	cla.nome_classe as json_classe_nome,
	subcla.id as json_subclasse_id,
	subcla.codigo_subclasse as json_subclasse_codigo,
	subcla.nome_subclasse as json_subclasse_nome,
	b.quantidade as volume_quantidade,
	b.unidade_medida_id as volume_id,
	um.sigla_unidade_medida as volume_sigla,
	um.nome_unidade_medida as volume_nome,
	b.valor_estimado as valores_estimado,
	b.valor_avaliado as valores_avaliado,
	a.data_apreensao as apreensao_data,
	a.local_apreensao as apreensao_local,
	a.id as apreensao_id,
	ta.id as apreensao_tipo_id,
	ta.nome_tipo as apreensao_nome,
	s.id as json_apreensao_situacao_situacao_id,
	s.nome_situacao as json_apreensao_situacao_nome,
	a.auto_apreensao_emitido as emitido,
	a.comprovante_apreensao_emitido as comprovante_emitido,
	a.responsavel as nome_responsavel,
	a.cpf_responsavel as cpf,
	bpv.orgao_julgador_id as idOrgaoJulgador,
	bpv.id as json_processos_vinculados_id,
	bpv.numero_processo as json_numeroProcesso,
	bpv.ctr_dth_inc as json_processo_data_de_inclusão,
	bpv.ctr_dth_atu as json_processo_data_de_atualizacao,
	bpv.nivel_sigilo as json_processo_nivel_sigilo,
	bpv.status as json_processo_status_verificado,
	bpv.principal as json_processo_principal,
    b.*,
    a.*,
    ta.*,
    apv.*,
    bpv.*
FROM snba.bem AS b
JOIN snba.bem_especificacao AS be 
    ON b.padrao_especificacao_id = be.id
JOIN snba.situacao AS s 
    ON s.id = b.situacao_id
JOIN snba.subclasse AS subcla 
    ON subcla.id = b.subclasse_id 
JOIN snba.classe AS cla 
    ON cla.id = subcla.classe_id
JOIN snba.categoria AS c 
    ON c.id = cla.categoria_id
JOIN snba.unidade_medida AS um 
    ON um.id = b.unidade_medida_id
JOIN snba.apreensao AS a
    ON a.id = b.apreensao_id 
JOIN snba.tipo_apreensao AS ta
    ON a.tipo_apreensao_id = ta.id 
JOIN snba.apreensao_processo_vinculado AS apv
    ON apv.apreensao_id = a.id 
JOIN snba.bem_processo_vinculado as bpv 
    ON bpv.bem_id = b.id     
WHERE b.id = 530792 
  AND b.padrao_especificacao_id = 261 and bpv.id = 464894 and bpv.numero_processo = '0823054-44.2021.8.15.2002';

		0823054-44.2021.8.15.2002
		
SELECT
    oc.id AS orgao_julgador_id,
    oc.nome AS orgao_julgador_nome,
    toc.nome AS segmento_justica,
    pai.nome AS jurisdicao_nome,
    oc.excluido
FROM apreensao_processo_vinculado apv
LEFT JOIN orgao_corporativo oc ON oc.id = apv.orgao_julgador_id
LEFT JOIN tipo_orgao_corporativo toc ON toc.id = oc.tipo_orgao_corporativo_id
LEFT JOIN orgao_corporativo pai ON pai.id = oc.pai_id
WHERE oc.excluido = FALSE;		
		
		
		
		
		
		
		
		

SELECT DISTINCT
    b.id AS json_id,
    b.ctr_dth_inc AS json_dataHoraInclusaoSngb,
    b.ctr_dth_atu AS json_dataHoraAtualizacaoSngb,
    b.data_restricao AS json_data_restricao,
    be.id AS json_especificacao_id,
    be.nome_atributo AS json_especificacao_nome,
    b.texto_especificacao_bem AS json_especificacao_texto,
    b.observacoes AS json_observacoes,
    s.id AS json_situacao_situacao_id,
    s.nome_situacao AS json_situacao_nome,
    c.id AS json_categoria_id,
    c.codigo_categoria AS json_categoria_codigo,
    c.nome_categoria AS json_categoria_nome,
    cla.id AS json_classe_id,
    cla.codigo_classe AS json_classe_codigo,
    cla.nome_classe AS json_classe_nome,
    subcla.id AS json_subclasse_id,
    subcla.codigo_subclasse AS json_subclasse_codigo,
    subcla.nome_subclasse AS json_subclasse_nome,
    b.quantidade AS volume_quantidade,
    b.unidade_medida_id AS volume_id,
    um.sigla_unidade_medida AS volume_sigla,
    um.nome_unidade_medida AS volume_nome,
    b.valor_estimado AS valores_estimado,
    b.valor_avaliado AS valores_avaliado,

    -- 🔍 Verificação de existência de cada JOIN
    CASE WHEN be.id IS NOT NULL THEN 'OK' ELSE 'FALHA' END AS join_bem_especificacao,
    CASE WHEN s.id IS NOT NULL THEN 'OK' ELSE 'FALHA' END AS join_situacao,
    CASE WHEN subcla.id IS NOT NULL THEN 'OK' ELSE 'FALHA' END AS join_subclasse,
    CASE WHEN cla.id IS NOT NULL THEN 'OK' ELSE 'FALHA' END AS join_classe,
    CASE WHEN c.id IS NOT NULL THEN 'OK' ELSE 'FALHA' END AS join_categoria,
    CASE WHEN um.id IS NOT NULL THEN 'OK' ELSE 'FALHA' END AS join_unidade_medida,
    CASE WHEN a.id IS NOT NULL THEN 'OK' ELSE 'FALHA' END AS join_apreensao,
    CASE WHEN ta.id IS NOT NULL THEN 'OK' ELSE 'FALHA' END AS join_tipo_apreensao,
    CASE WHEN apv.id IS NOT NULL THEN 'OK' ELSE 'FALHA' END AS join_apreensao_processo_vinculado,
    CASE WHEN bpv.id IS NOT NULL THEN 'OK' ELSE 'FALHA' END AS join_bem_processo_vinculado,

    -- 🔁 Mantém todas as colunas originais
    b.*
FROM snba.bem AS b
LEFT JOIN snba.bem_especificacao AS be ON b.padrao_especificacao_id = be.id
LEFT JOIN snba.situacao AS s ON s.id = b.situacao_id
LEFT JOIN snba.subclasse AS subcla ON subcla.id = b.subclasse_id
LEFT JOIN snba.classe AS cla ON cla.id = subcla.classe_id
LEFT JOIN snba.categoria AS c ON c.id = cla.categoria_id
LEFT JOIN snba.unidade_medida AS um ON um.id = b.unidade_medida_id
LEFT JOIN snba.apreensao AS a ON a.id = b.apreensao_id
LEFT JOIN snba.tipo_apreensao AS ta ON a.tipo_apreensao_id = ta.id
LEFT JOIN snba.apreensao_processo_vinculado AS apv ON apv.apreensao_id = a.id
LEFT JOIN snba.bem_processo_vinculado AS bpv ON bpv.bem_id = b.id
WHERE b.id = 340473;




SELECT
    COUNT(*) AS total_linhas,
    COUNT(DISTINCT b.id) AS distinct_b_id,
    COUNT(DISTINCT be.id) AS distinct_be_id,
    COUNT(DISTINCT s.id) AS distinct_s_id,
    COUNT(DISTINCT c.id) AS distinct_c_id,
    COUNT(DISTINCT cla.id) AS distinct_cla_id,
    COUNT(DISTINCT subcla.id) AS distinct_subcla_id,
    COUNT(DISTINCT a.id) AS distinct_a_id,
    COUNT(DISTINCT ta.id) AS distinct_ta_id,
    COUNT(DISTINCT apv.id) AS distinct_apv_id,
    COUNT(DISTINCT bpv.id) AS distinct_bpv_id
FROM snba.bem AS b
JOIN snba.bem_especificacao AS be ON b.padrao_especificacao_id = be.id
JOIN snba.situacao AS s ON s.id = b.situacao_id
JOIN snba.subclasse AS subcla ON subcla.id = b.subclasse_id
JOIN snba.classe AS cla ON cla.id = subcla.classe_id
JOIN snba.categoria AS c ON c.id = cla.categoria_id
JOIN snba.unidade_medida AS um ON um.id = b.unidade_medida_id
JOIN snba.apreensao AS a ON a.id = b.apreensao_id
JOIN snba.tipo_apreensao AS ta ON a.tipo_apreensao_id = ta.id
JOIN snba.apreensao_processo_vinculado AS apv ON apv.apreensao_id = a.id
JOIN snba.bem_processo_vinculado as bpv ON bpv.bem_id = b.id
WHERE b.id = 530792;











SELECT DISTINCT
	b.id as json_id,
	b.ctr_dth_inc as json_dataHoraInclusaoSngb,
	b.ctr_dth_atu as json_dataHoraAtualizacaoSngb,
	b.data_restricao as json_data_restricao,
	be.id as json_especificacao_id,
	be.nome_atributo as json_especificacao_nome,
	b.texto_especificacao_bem as json_especificacao_texto,
	b.observacoes as json_observacoes,
	s.id as json_situacao_situacao_id,
	s.nome_situacao as json_situacao_nome,
	c.id as json_categoria_id,
	c.codigo_categoria as json_categoria_codigo,
	c.nome_categoria as json_categoria_nome,
	cla.id as json_classe_id,
	cla.codigo_classe as json_classe_codigo,
	cla.nome_classe as json_classe_nome,
	subcla.id as json_subclasse_id,
	subcla.codigo_subclasse as json_subclasse_codigo,
	subcla.nome_subclasse as json_subclasse_nome,
	b.quantidade as volume_quantidade,
	b.unidade_medida_id as volume_id,
	um.sigla_unidade_medida as volume_sigla,
	um.nome_unidade_medida as volume_nome,
	b.valor_estimado as valores_estimado,
	b.valor_avaliado as valores_avaliado,
	a.data_apreensao as apreensao_data,
	a.local_apreensao as apreensao_local,
	a.id as apreensao_id,
	ta.id as apreensao_tipo_id,
	ta.nome_tipo as apreensao_nome,
	s.id as json_apreensao_situacao_situacao_id,
	s.nome_situacao as json_apreensao_situacao_nome,
	a.auto_apreensao_emitido as emitido,
	a.comprovante_apreensao_emitido as comprovante_emitido,
	a.responsavel as nome_responsavel,
	a.cpf_responsavel as cpf,
	bpv.orgao_julgador_id as idOrgaoJulgador,
	bpv.id as json_processos_vinculados_id,
	bpv.numero_processo as json_numeroProcesso,
	bpv.ctr_dth_inc as json_processo_data_de_inclusão,
	bpv.ctr_dth_atu as json_processo_data_de_atualizacao,
	bpv.nivel_sigilo as json_processo_nivel_sigilo,
	bpv.status as json_processo_status_verificado,
	bpv.principal as json_processo_principal,
	*
FROM snba.bem AS b
JOIN snba.bem_especificacao AS be 
    ON b.padrao_especificacao_id = be.id
JOIN snba.situacao AS s 
    ON s.id = b.situacao_id
JOIN snba.subclasse AS subcla 
    ON subcla.id = b.subclasse_id 
JOIN snba.classe AS cla 
    ON cla.id = subcla.classe_id
JOIN snba.categoria AS c 
    ON c.id = cla.categoria_id
JOIN snba.unidade_medida AS um 
    ON um.id = b.unidade_medida_id
JOIN snba.apreensao AS a
    ON a.id = b.apreensao_id 
JOIN snba.tipo_apreensao AS ta
    ON a.tipo_apreensao_id = ta.id 
JOIN snba.apreensao_processo_vinculado AS apv
    ON apv.apreensao_id = a.id 
JOIN snba.bem_processo_vinculado as bpv 
    ON bpv.bem_id = b.id
join snba.padrao_especificacao as pe
	on b.padrao_especificacao_id = pe.id and b.subclasse_id = subcla.id and pe.situacao_id = s.id 
WHERE b.id = 530792 



  AND b.padrao_especificacao_id = 261 and bpv.id = 464894 

  
  and bpv.numero_processo = '0823054-44.2021.8.15.2002';

  
  
CREATE TEMP TABLE temp_bem_detalhes AS
SELECT 
	b.id AS json_id,
	b.ctr_dth_inc AS json_dataHoraInclusaoSngb,
	b.ctr_dth_atu AS json_dataHoraAtualizacaoSngb,
	b.data_restricao AS json_data_restricao,
	be.id AS json_especificacao_id,
	be.nome_atributo AS json_especificacao_nome,
	b.texto_especificacao_bem AS json_especificacao_texto,
	b.observacoes AS json_observacoes,
	s.id AS json_situacao_situacao_id,
	s.nome_situacao AS json_situacao_nome,
	c.id AS json_categoria_id,
	c.codigo_categoria AS json_categoria_codigo,
	c.nome_categoria AS json_categoria_nome,
	cla.id AS json_classe_id,
	cla.codigo_classe AS json_classe_codigo,
	cla.nome_classe AS json_classe_nome,
	subcla.id AS json_subclasse_id,
	subcla.codigo_subclasse AS json_subclasse_codigo,
	subcla.nome_subclasse AS json_subclasse_nome,
	b.quantidade AS volume_quantidade,
	b.unidade_medida_id AS volume_id,
	um.sigla_unidade_medida AS volume_sigla,
	um.nome_unidade_medida AS volume_nome,
	b.valor_estimado AS valores_estimado,
	b.valor_avaliado AS valores_avaliado,
	a.data_apreensao AS apreensao_data,
	a.local_apreensao AS apreensao_local,
	a.id AS apreensao_id,
	a.ctr_dth_inc as apreensao_datahorainclusao,
	a.ctr_dth_atu as apreensao_datahoraatualizacao,
	ta.id AS apreensao_tipo_id,
	ta.nome_tipo AS apreensao_nome,
	a.auto_apreensao_emitido AS emitido,
	a.comprovante_apreensao_emitido AS comprovante_emitido,
	a.responsavel AS nome_responsavel,
	a.cpf_responsavel AS cpf,
	bpv.orgao_julgador_id AS idOrgaoJulgador,
	bpv.id AS json_processos_vinculados_id,
	bpv.numero_processo AS json_numeroProcesso,
	bpv.ctr_dth_inc AS json_processo_data_de_inclusao,
	bpv.ctr_dth_atu AS json_processo_data_de_atualizacao,
	bpv.nivel_sigilo AS json_processo_nivel_sigilo,
	bpv.status AS json_processo_status_verificado,
	bpv.principal AS json_processo_principal
FROM snba.bem AS b
JOIN snba.bem_especificacao AS be ON b.padrao_especificacao_id = be.id
JOIN snba.situacao AS s ON s.id = b.situacao_id
JOIN snba.subclasse AS subcla ON subcla.id = b.subclasse_id 
JOIN snba.classe AS cla ON cla.id = subcla.classe_id
JOIN snba.categoria AS c ON c.id = cla.categoria_id
JOIN snba.unidade_medida AS um ON um.id = b.unidade_medida_id
JOIN snba.apreensao AS a ON a.id = b.apreensao_id 
JOIN snba.tipo_apreensao AS ta ON a.tipo_apreensao_id = ta.id 
JOIN snba.apreensao_processo_vinculado AS apv ON apv.apreensao_id = a.id 
JOIN snba.bem_processo_vinculado AS bpv ON bpv.bem_id = b.id
JOIN snba.padrao_especificacao AS pe 
	ON b.padrao_especificacao_id = pe.id 
	AND b.subclasse_id = subcla.id 
	AND pe.situacao_id = s.id 
WHERE b.id = 530792
GROUP BY 
	b.id, b.ctr_dth_inc, b.ctr_dth_atu, b.data_restricao,
	be.id, be.nome_atributo,
	b.texto_especificacao_bem, b.observacoes,
	s.id, s.nome_situacao,
	c.id, c.codigo_categoria, c.nome_categoria,
	cla.id, cla.codigo_classe, cla.nome_classe,
	subcla.id, subcla.codigo_subclasse, subcla.nome_subclasse,
	b.quantidade, b.unidade_medida_id, um.sigla_unidade_medida, um.nome_unidade_medida,
	b.valor_estimado, b.valor_avaliado,
	a.data_apreensao, a.local_apreensao, a.id, a.ctr_dth_inc, a.ctr_dth_atu,
	ta.id, ta.nome_tipo,
	a.auto_apreensao_emitido, a.comprovante_apreensao_emitido,
	a.responsavel, a.cpf_responsavel,
	bpv.orgao_julgador_id, bpv.id, bpv.numero_processo,
	bpv.ctr_dth_inc, bpv.ctr_dth_atu, bpv.nivel_sigilo,
	bpv.status, bpv.principal;


	a.ctr_dth_inc as apreensao_dataHoraInclusao,
	a.ctr_dth_atu

select * from temp_bem_detalhes

SELECT 
    b.json_id,
    json_build_object(
        'id', b.json_id,
        'versao', '0.3.0',
        'dataHoraAtualizacao', 'Nao sei de onde vem essa data'::TEXT,
        'dataHoraInclusaoSngb', b.json_dataHoraInclusaoSngb,
        'dataHoraAtualizacaoSngb', b.json_dataHoraAtualizacaoSngb,
        'dataRestricao', b.json_data_restricao,
        'especificacao', json_build_object(
            'padrao', json_build_object(
                'idSngb', b.json_especificacao_id,
                'nome', b.json_especificacao_nome
            ),
            'texto', b.json_especificacao_texto,
            'observacoes', b.json_observacoes
        ),
        'situacao', json_build_object(
            'idSngb', b.json_situacao_situacao_id,
            'nome', b.json_situacao_nome
        ),
        'classificacao', json_build_object(
            'categoria', json_build_object(
                'idSngb', b.json_categoria_id,
                'codigo', b.json_categoria_codigo,
                'nome', b.json_categoria_nome
            ),
            'classe', json_build_object(
                'idSngb', b.json_classe_id,
                'codigo', b.json_classe_codigo,
                'nome', b.json_classe_nome
            ),
            'subclasse', json_build_object(
                'idSngb', b.json_subclasse_id,
                'codigo', b.json_subclasse_codigo,
                'nome', b.json_subclasse_nome
            )
        ),
        'volume', json_build_object(
            'quantidade', b.volume_quantidade,
            'unidadeMedida', json_build_object(
                'idSngb', b.volume_id,
                'sigla', b.volume_sigla,
                'nome', b.volume_nome
            )
        ),
        'valores', json_build_object(
            'estimado', b.valores_estimado,
            'avaliado', b.valores_avaliado
        ),
        'detentor', json_build_object(
            'orgao', json_build_object(
                'idSngb', 'a.orgao_id',
                'nome', 'b.nome_orgao_detentor'  -- ⚠️ deve vir de JOIN ou estar na tabela b
            ),
            'unidade', json_build_object(
                'idSngb', 'b.unidade_detentora_id',
                'nome', 'b.nome_orgao_detentor'
            ),
            'localArmazenamento', json_build_object(
                'idSngb', 'a.local_armazenamento_id',
                'nome', 'a.nome',
                'tipo', json_build_object(
                    'idSngb', 'a.id_2',
                    'codigo', 'a.codigo',
                    'nome', 'a.nome_1'
                ),
                'endereco', json_build_object(
                    'idSngb', 'a.idendereco',
                    'logradouro', 'a.logradouro',
                    'numero', 'a.numero',
                    'complemento', 'a.complemento',
                    'bairro', 'a.bairro',
                    'cep', 'a.cep',
                    'localidade', json_build_object(
                        'codigo', 'a.idmunicipio',
                        'descricao', 'a.municipio'
                    ),
                    'uf', json_build_object(
                        'idSngb', 'a.codigo_uf_ibge',
                        'sigla', 'a.sigla_uf',
                        'nome', 'UPPER(a.nome_uf)'
                    ),
                    'pais', json_build_object(
                        'idSngb', 'a.idpais',
                        'sigla', 'BR',
                        'nome', 'UPPER(a.pais)'
                    ),
                    'excluido', FALSE
                ),
                'excluido', FALSE
            )
        ),
        'processosVinculados', COALESCE(
            json_agg(
                json_build_object(
                    'idSngb', b.json_processos_vinculados_id,
                    'numeroProcesso', b.json_numeroProcesso,
                    'dataHoraInclusao', b.json_processo_data_de_inclusao,
                    'dataHoraAtualizacao', b.json_processo_data_de_atualizacao,
                    'nivelSigilo', COALESCE(b.json_processo_nivel_sigilo, 0),
                    'status', b.json_processo_status_verificado,
                    'principal', b.json_processo_principal,
                    'orgaoJulgador', json_build_object(
                        'tribunal', json_build_object(
                            'segmento', 'JUSTICA_ESTADUAL',
                            'sigla', 'TJPB',
                            'nome', 'Tribunal de Justiça do Estado da Paraíba'
                        ),
                        'id', b.idOrgaoJulgador,
                        'nome', '4ª VARA CRIMINAL DE JOÃO PESSOA',
                        'jurisdicao', json_build_object(
                            'id', 1334,
                            'nome', 'JOAO PESSOA'
                        )
                    ),
                    'orgaoJulgadorColegiado', json_build_object(
                        'id', NULL,
                        'nome', NULL
                    ),
                    'excluido', false
                )
                ORDER BY b.json_processo_principal DESC, b.json_processos_vinculados_id
            ) FILTER (WHERE b.json_processos_vinculados_id IS NOT NULL),
            '[]'::json
        ),
        'excluido', false
    ) AS json_completo
FROM temp_bem_detalhes AS b
--LEFT JOIN acesso AS a ON b.orgao_detentor_id = a.orgao_id
GROUP BY 
    b.json_id,
    b.json_dataHoraInclusaoSngb,
    b.json_dataHoraAtualizacaoSngb,
    b.json_data_restricao,
    b.json_especificacao_id,
    b.json_especificacao_nome,
    b.json_especificacao_texto,
    b.json_observacoes,
    b.json_situacao_situacao_id,
    b.json_situacao_nome,
    b.json_categoria_id,
    b.json_categoria_codigo,
    b.json_categoria_nome,
    b.json_classe_id,
    b.json_classe_codigo,
    b.json_classe_nome,
    b.json_subclasse_id,
    b.json_subclasse_codigo,
    b.json_subclasse_nome,
    b.volume_quantidade,
    b.volume_id,
    b.volume_sigla,
    b.volume_nome,
    b.valores_estimado,
    b.valores_avaliado;



















SELECT 
    json_build_object(
        'processosVinculados', json_agg(
            json_build_object(
                'idSngb', bpv.id,
                'numeroProcesso', bpv.numero_processo,
                'dataHoraInclusao', bpv.ctr_dth_inc,
                'dataHoraAtualizacao', bpv.ctr_dth_atu,
                'nivelSigilo', COALESCE(bpv.nivel_sigilo, 0),
                'status', bpv.status,
                'principal', bpv.principal,
                'orgaoJulgador', json_build_object(
                    'tribunal', json_build_object(
                        'segmento', 'JUSTICA_ESTADUAL',
                        'sigla', 'TJPB',
                        'nome', 'Tribunal de Justiça do Estado da Paraíba'
                    ),
                    'id', bpv.orgao_julgador_id,
                    'nome', '4ª VARA CRIMINAL DE JOÃO PESSOA',  -- ⚠️ substitua por JOIN real
                    'jurisdicao', json_build_object(
                        'id', 1334,                               -- ⚠️ id da jurisdição
                        'nome', 'JOAO PESSOA'                     -- ⚠️ nome da cidade
                    )
                ),
                'orgaoJulgadorColegiado', json_build_object(
                    'id', NULL,
                    'nome', NULL
                ),
                'excluido', false
            )
            ORDER BY bpv.principal DESC, bpv.id  -- principal primeiro
        ),
        'excluido', false
    ) AS resultado_json

WHERE b.id = 530792
GROUP BY b.id;



SELECT 
  bem.id,
  json_build_object(
    'id', bem.id,
    'versao', '0.3.0',
    'dataHoraAtualizacao', to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS.MS'),
    'dataHoraInclusaoSngb', bem.ctr_dth_inc,
    'dataHoraAtualizacaoSngb', bem.ctr_dth_atu,
    'dataRestricao', bem.data_restricao,

    -- Especificações
    'especificacoes', (
        SELECT json_build_object(
            'padrao', json_build_object(
                'idSngb', bem.padrao_especificacao_id,
                'nome', bem_especificacao.nome_atributo
            ),
            'texto', bem.texto_especificacao_bem,
            'observacoes', bem.observacoes
        )
        FROM snba.bem_especificacao AS bem_especificacao
        WHERE bem_especificacao.bem_id = bem.id
    ),

    -- Situação
    'situacao', (
        SELECT json_build_object(
            'padrao', json_build_object(
                'idSngb', situacao.id,
                'nome', situacao.nome_situacao
            )
        )
        FROM snba.situacao AS situacao
        WHERE situacao.id = bem.situacao_id
    ),

    -- Classificação
    'classificacao', (
        SELECT json_build_object(
            'categoria', json_build_object(
                'idSngb', categoria.id,
                'codigo', categoria.codigo_categoria,
                'nome', categoria.nome_categoria
            ),
            'classe', json_build_object(
                'idSngb', classe.id,
                'codigo', classe.codigo_classe,
                'nome', classe.nome_classe
            ),
            'subclasse', json_build_object(
                'idSngb', subclasse.id,
                'codigo', subclasse.codigo_subclasse,
                'nome', subclasse.nome_subclasse
            )
        )
        FROM snba.subclasse AS subclasse
        JOIN snba.classe AS classe ON classe.id = subclasse.classe_id
        JOIN snba.categoria AS categoria ON categoria.id = classe.categoria_id
        WHERE subclasse.id = bem.subclasse_id
    ),

    -- Volume
    'volume', (
        SELECT json_build_object(
            'quantidade', bem.quantidade,
            'unidadeMedida', json_build_object(
                'idSngb', bem.unidade_medida_id,
                'sigla', unidade_medida.sigla_unidade_medida,
                'nome', unidade_medida.nome_unidade_medida
            )
        )
        FROM snba.unidade_medida as unidade_medida
        WHERE bem.unidade_medida_id = unidade_medida.id
    ),

    -- Valores
    'valores', json_build_object(
        'estimado', NULL,
        'avaliado', NULL
    ),

    -- Detentor
    'detentor', (
        SELECT CASE
            WHEN apreensao.id IS NOT NULL THEN
                json_build_object(
                    'orgao', json_build_object(
                        'idSngb', COALESCE(apreensao.orgao_detentor_id, bem.orgao_detentor_id) ,
                        'nome', apreensao.nome_orgao_detentor
                    ),
                    'unidade', CASE 
                        WHEN apreensao.orgao_detentor_id IS NOT NULL THEN
                            json_build_object(
                                'idSngb', apreensao.orgao_detentor_id,
                                'nome', apreensao.nome_orgao_detentor
                            )
                        ELSE NULL
                    END,
                    'localArmazenamento', 
                    --CASE 
                        --WHEN apreensao.local_id IS NOT NULL THEN
                            json_build_object(
                                'idSngb', 'apreensao.local_id',
                                'nome', 'apreensao.local_nome',
                                'tipo', json_build_object(
                                    'idSngb', 'apreensao.tipo_id',
                                    'codigo', 'apreensao.tipo_codigo',
                                    'nome', 'apreensao.tipo_nome'
                                ),
                                'endereco', json_build_object(
                                    'idSngb', 'apreensao.endereco_id',
                                    'idEndereco', 'apreensao.idEndereco',
                                    'logradouro', 'apreensao.logradouro',
                                    'numero', 'apreensao.numero',
                                    'complemento', 'apreensao.complemento',
                                    'bairro', 'apreensao.bairro',
                                    'cep', 'apreensao.cep',
                                    'pais', 'apreensao.pais_nome',
                                    'localidade', json_build_object(
                                        'codigo', 'apreensao.localidade_codigo',
                                        'descricao', 'apreensao.localidade_descricao'
                                    ),
                                    'uf', json_build_object(
                                        'idSngb', 'apreensao.uf_id',
                                        'sigla', 'apreensao.uf_sigla',
                                        'nome', 'apreensao.uf_nome'
                                    ),
                                    'pais', json_build_object(
                                        'idSngb', 'apreensao.pais_id',
                                        'sigla', 'apreensao.pais_sigla',
                                        'nome', 'apreensao.pais_nome'
                                    ),
                                    'excluido', 'apreensao.excluido'
                                ),
                                'excluido', 'apreensao.local_excluido'
                            )
                        --ELSE NULL
                    --END
                )
            ELSE NULL
        END
        FROM snba.apreensao AS apreensao
        WHERE apreensao.id = bem.apreensao_id
    )

  ) AS json_output
FROM snba.bem AS bem
WHERE bem.id = 340473;


340473;


530792

	um.sigla_unidade_medida AS volume_sigla,
	um.nome_unidade_medida AS volume_nome,

	cla.id AS json_classe_id,
	cla.codigo_classe AS json_classe_codigo,
	cla.nome_classe AS json_classe_nome,
	
	subcla.id AS json_subclasse_id,
	subcla.codigo_subclasse AS json_subclasse_codigo,
	subcla.nome_subclasse AS json_subclasse_nome,



snba.situacao AS s ON s.id = b.situacao_id




SQL Error [42601]: ERROR: syntax error at or near ")"
  Position: 557

Error position: line: 586 pos: 556






-- VERSAO FINAL

SELECT 
    bem.id,
    json_build_object(
        'id', bem.id,
        'versao', '0.3.0',
        'dataHoraAtualizacao', to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS.MS'),
        'dataHoraInclusaoSngb', bem.ctr_dth_inc,
        'dataHoraAtualizacaoSngb', bem.ctr_dth_atu,
        'dataRestricao', bem.data_restricao,

        -- Especificações
        'especificacoes', (
            SELECT json_build_object(
                'padrao', json_build_object(
                    'idSngb', bem.padrao_especificacao_id,
                    'nome', bem_especificacao.nome_atributo
                ),
                'texto', bem.texto_especificacao_bem,
                'observacoes', bem.observacoes
            )
            FROM snba.bem_especificacao AS bem_especificacao
            WHERE bem_especificacao.bem_id = bem.id
        ),

        -- Situação
        'situacao', (
            SELECT json_build_object(
                'padrao', json_build_object(
                    'idSngb', situacao.id,
                    'nome', situacao.nome_situacao
                )
            )
            FROM snba.situacao AS situacao
            WHERE situacao.id = bem.situacao_id
        ),

        -- Classificação
        'classificacao', (
            SELECT json_build_object(
                'categoria', json_build_object(
                    'idSngb', categoria.id,
                    'codigo', categoria.codigo_categoria,
                    'nome', categoria.nome_categoria
                ),
                'classe', json_build_object(
                    'idSngb', classe.id,
                    'codigo', classe.codigo_classe,
                    'nome', classe.nome_classe
                ),
                'subclasse', json_build_object(
                    'idSngb', subclasse.id,
                    'codigo', subclasse.codigo_subclasse,
                    'nome', subclasse.nome_subclasse
                )
            )
            FROM snba.subclasse AS subclasse
            JOIN snba.classe AS classe ON classe.id = subclasse.classe_id
            JOIN snba.categoria AS categoria ON categoria.id = classe.categoria_id
            WHERE subclasse.id = bem.subclasse_id
        ),

        -- Volume
        'volume', (
            SELECT json_build_object(
                'quantidade', bem.quantidade,
                'unidadeMedida', json_build_object(
                    'idSngb', bem.unidade_medida_id,
                    'sigla', unidade_medida.sigla_unidade_medida,
                    'nome', unidade_medida.nome_unidade_medida
                )
            )
            FROM snba.unidade_medida as unidade_medida
            WHERE bem.unidade_medida_id = unidade_medida.id
        ),

        -- Valores
        'valores', json_build_object(
            'estimado', NULL,
            'avaliado', NULL
        ),

        -- Detentor
        'detentor', json_build_object(
            'orgao', json_build_object(
                'idSngb', COALESCE(
                    (SELECT apreensao.orgao_detentor_id 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.orgao_detentor_id
                ),
                'nome', COALESCE(
                    (SELECT apreensao.nome_orgao_detentor 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.nome_orgao_detentor
                )
            ),
            'unidade', json_build_object(
                'idSngb', COALESCE(
                    (SELECT apreensao.orgao_detentor_id 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.orgao_detentor_id
                ),
                'nome', COALESCE(
                    (SELECT apreensao.nome_orgao_detentor 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.nome_orgao_detentor
                )
            ),
            'localArmazenamento', NULL -- Mantido NULL, pois colunas não existem
        )

    ) AS json_output
FROM snba.bem AS bem
WHERE bem.id = 340473;



SELECT 
    bem.id,
    json_build_object(
        'id', bem.id,
        'versao', '0.3.0',
        'dataHoraAtualizacao', to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS.MS'),
        'dataHoraInclusaoSngb', bem.ctr_dth_inc,
        'dataHoraAtualizacaoSngb', bem.ctr_dth_atu,
        'dataRestricao', bem.data_restricao,

        -- Especificações
        'especificacoes', (
            SELECT json_build_object(
                'padrao', json_build_object(
                    'idSngb', bem.padrao_especificacao_id,
                    'nome', bem_especificacao.nome_atributo
                ),
                'texto', bem.texto_especificacao_bem,
                'observacoes', bem.observacoes
            )
            FROM snba.bem_especificacao AS bem_especificacao
            WHERE bem_especificacao.bem_id = bem.id
        ),

        -- Situação
        'situacao', (
            SELECT json_build_object(
                'padrao', json_build_object(
                    'idSngb', situacao.id,
                    'nome', situacao.nome_situacao
                )
            )
            FROM snba.situacao AS situacao
            WHERE situacao.id = bem.situacao_id
        ),

        -- Classificação
        'classificacao', (
            SELECT json_build_object(
                'categoria', json_build_object(
                    'idSngb', categoria.id,
                    'codigo', categoria.codigo_categoria,
                    'nome', categoria.nome_categoria
                ),
                'classe', json_build_object(
                    'idSngb', classe.id,
                    'codigo', classe.codigo_classe,
                    'nome', classe.nome_classe
                ),
                'subclasse', json_build_object(
                    'idSngb', subclasse.id,
                    'codigo', subclasse.codigo_subclasse,
                    'nome', subclasse.nome_subclasse
                )
            )
            FROM snba.subclasse AS subclasse
            JOIN snba.classe AS classe ON classe.id = subclasse.classe_id
            JOIN snba.categoria AS categoria ON categoria.id = classe.categoria_id
            WHERE subclasse.id = bem.subclasse_id
        ),

        -- Volume
        'volume', (
            SELECT json_build_object(
                'quantidade', bem.quantidade,
                'unidadeMedida', json_build_object(
                    'idSngb', bem.unidade_medida_id,
                    'sigla', unidade_medida.sigla_unidade_medida,
                    'nome', unidade_medida.nome_unidade_medida
                )
            )
            FROM snba.unidade_medida as unidade_medida
            WHERE bem.unidade_medida_id = unidade_medida.id
        ),

        -- Valores
        'valores', json_build_object(
            'estimado', NULL,
            'avaliado', NULL
        ),

        -- Detentor
        'detentor', json_build_object(
            'orgao', json_build_object(
                'idSngb', COALESCE(
                    (SELECT apreensao.orgao_detentor_id 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.orgao_detentor_id
                ),
                'nome', COALESCE(
                    (SELECT apreensao.nome_orgao_detentor 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.nome_orgao_detentor
                )
            ),
            'unidade', json_build_object(
                'idSngb', COALESCE(
                    (SELECT apreensao.orgao_detentor_id 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.orgao_detentor_id
                ),
                'nome', COALESCE(
                    (SELECT apreensao.nome_orgao_detentor 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.nome_orgao_detentor
                )
            ),
            'localArmazenamento', json_build_object(
                'idSngb', 'acesso_armazenamento_id',
                'nome', 'acesso_nome_armazenamento',
                'tipo', json_build_object(
                    'idSngb', 'acesso_tipo_id',
                    'codigo', 'acesso_codigo',
                    'nome', 'acesso_nome'
                ),
                'endereco', json_build_object(
                    'idSngb', 'acesso_id',
                    'idEndereco', 'acesso_endereco_id',
                    'logradouro', 'acesso_logradouro',
                    'numero', 'acesso_numero',
                    'complemento', 'acesso_complemento',
                    'bairro', 'acesso_bairro',
                    'cep', 'acesso_cep',
                    'pais', 'acesso_pais',
                    'localidade', json_build_object(
                        'codigo', 'acesso_codigo_localidade',
                        'descricao', 'acesso_descricao'
                    ),
                    'uf', json_build_object(
                        'idSngb', 'acesso_municio_id',
                        'sigla', 'acesso_municipio_sigla',
                        'nome', 'acesso_municipio_nome'
                    ),
                    'pais', json_build_object(
                        'idSngb', 'acesso_pais_id',
                        'sigla', 'acesso_sigla',
                        'nome', 'acesso_nome_pais'
                    ),
                    'excluido', false
                ),
                'excluido', false
            )
        )
    ) AS json_output
FROM snba.bem AS bem
WHERE bem.id = 340473; -- ou 530792







SELECT 
    bem.id AS bem_id,
    bem.ctr_dth_inc AS dataHoraInclusaoSngb,
    bem.ctr_dth_atu AS dataHoraAtualizacaoSngb,
    now() AS dataHoraAtualizacao,
    bem.data_restricao AS dataRestricao,
    
    -- Especificações
    bem.padrao_especificacao_id AS especificacoes_padrao_idSngb,
    bem_especificacao.nome_atributo AS especificacoes_padrao_nome,
    bem.texto_especificacao_bem AS especificacoes_texto,
    bem.observacoes AS especificacoes_observacoes,
    
    -- Situação
    situacao.id AS situacao_padrao_idSngb,
    situacao.nome_situacao AS situacao_padrao_nome,
    
    -- Classificação
    categoria.id AS classificacao_categoria_idSngb,
    categoria.codigo_categoria AS classificacao_categoria_codigo,
    categoria.nome_categoria AS classificacao_categoria_nome,
    classe.id AS classificacao_classe_idSngb,
    classe.codigo_classe AS classificacao_classe_codigo,
    classe.nome_classe AS classificacao_classe_nome,
    subclasse.id AS classificacao_subclasse_idSngb,
    subclasse.codigo_subclasse AS classificacao_subclasse_codigo,
    subclasse.nome_subclasse AS classificacao_subclasse_nome,
    
    -- Volume
    bem.quantidade AS volume_quantidade,
    bem.unidade_medida_id AS volume_unidadeMedida_idSngb,
    unidade_medida.sigla_unidade_medida AS volume_unidadeMedida_sigla,
    unidade_medida.nome_unidade_medida AS volume_unidadeMedida_nome,
    
    -- Valores
    NULL::numeric AS valores_estimado,
    NULL::numeric AS valores_avaliado,
    
    -- Detentor
    COALESCE(apreensao.orgao_detentor_id, bem.orgao_detentor_id) AS detentor_orgao_idSngb,
    COALESCE(apreensao.nome_orgao_detentor, bem.nome_orgao_detentor) AS detentor_orgao_nome,
    COALESCE(apreensao.orgao_detentor_id, bem.orgao_detentor_id) AS detentor_unidade_idSngb,
    COALESCE(apreensao.nome_orgao_detentor, bem.nome_orgao_detentor) AS detentor_unidade_nome,
    
    -- Local de armazenamento
    apreensao.local_id AS detentor_localArmazenamento_idSngb,
    apreensao.local_nome AS detentor_localArmazenamento_nome,
    apreensao.tipo_id AS detentor_localArmazenamento_tipo_idSngb,
    apreensao.tipo_codigo AS detentor_localArmazenamento_tipo_codigo,
    apreensao.tipo_nome AS detentor_localArmazenamento_tipo_nome,
    apreensao.endereco_id AS detentor_localArmazenamento_endereco_idSngb,
    apreensao.idEndereco AS detentor_localArmazenamento_endereco_idEndereco,
    apreensao.logradouro AS detentor_localArmazenamento_endereco_logradouro,
    apreensao.numero AS detentor_localArmazenamento_endereco_numero,
    apreensao.complemento AS detentor_localArmazenamento_endereco_complemento,
    apreensao.bairro AS detentor_localArmazenamento_endereco_bairro,
    apreensao.cep AS detentor_localArmazenamento_endereco_cep,
    apreensao.pais_nome AS detentor_localArmazenamento_endereco_pais,
    apreensao.localidade_codigo AS detentor_localArmazenamento_endereco_localidade_codigo,
    apreensao.localidade_descricao AS detentor_localArmazenamento_endereco_localidade_descricao,
    apreensao.uf_id AS detentor_localArmazenamento_endereco_uf_idSngb,
    apreensao.uf_sigla AS detentor_localArmazenamento_endereco_uf_sigla,
    apreensao.uf_nome AS detentor_localArmazenamento_endereco_uf_nome,
    apreensao.pais_id AS detentor_localArmazenamento_endereco_pais_idSngb,
    apreensao.pais_sigla AS detentor_localArmazenamento_endereco_pais_sigla,
    apreensao.pais_nome AS detentor_localArmazenamento_endereco_pais_nome,
    apreensao.excluido AS detentor_localArmazenamento_endereco_excluido,
    apreensao.local_excluido AS detentor_localArmazenamento_excluido

FROM snba.bem AS bem
LEFT JOIN snba.bem_especificacao AS bem_especificacao 
    ON bem_especificacao.bem_id = bem.id
LEFT JOIN snba.situacao AS situacao 
    ON situacao.id = bem.situacao_id
LEFT JOIN snba.subclasse AS subclasse 
    ON subclasse.id = bem.subclasse_id
LEFT JOIN snba.classe AS classe 
    ON classe.id = subclasse.classe_id
LEFT JOIN snba.categoria AS categoria 
    ON categoria.id = classe.categoria_id
LEFT JOIN snba.unidade_medida AS unidade_medida 
    ON unidade_medida.id = bem.unidade_medida_id
LEFT JOIN snba.apreensao AS apreensao 
    ON apreensao.id = bem.apreensao_id
WHERE bem.id = 340473; -- ou 530792








SELECT 
    bem.id AS bem_id,
    bem.ctr_dth_inc AS dataHoraInclusaoSngb,
    bem.ctr_dth_atu AS dataHoraAtualizacaoSngb,
    now() AS dataHoraAtualizacao,
    bem.data_restricao AS dataRestricao,
    
    -- Especificações
    bem.padrao_especificacao_id AS especificacoes_padrao_idSngb,
    bem_especificacao.nome_atributo AS especificacoes_padrao_nome,
    bem.texto_especificacao_bem AS especificacoes_texto,
    bem.observacoes AS especificacoes_observacoes,
    
    -- Situação
    situacao.id AS situacao_padrao_idSngb,
    situacao.nome_situacao AS situacao_padrao_nome,
    
    -- Classificação
    categoria.id AS classificacao_categoria_idSngb,
    categoria.codigo_categoria AS classificacao_categoria_codigo,
    categoria.nome_categoria AS classificacao_categoria_nome,
    classe.id AS classificacao_classe_idSngb,
    classe.codigo_classe AS classificacao_classe_codigo,
    classe.nome_classe AS classificacao_classe_nome,
    subclasse.id AS classificacao_subclasse_idSngb,
    subclasse.codigo_subclasse AS classificacao_subclasse_codigo,
    subclasse.nome_subclasse AS classificacao_subclasse_nome,
    
    -- Volume
    bem.quantidade AS volume_quantidade,
    bem.unidade_medida_id AS volume_unidadeMedida_idSngb,
    unidade_medida.sigla_unidade_medida AS volume_unidadeMedida_sigla,
    unidade_medida.nome_unidade_medida AS volume_unidadeMedida_nome,
    
    -- Valores
    NULL::numeric AS valores_estimado,
    NULL::numeric AS valores_avaliado,
    
    -- Detentor
    COALESCE(apreensao.orgao_detentor_id, bem.orgao_detentor_id) AS detentor_orgao_idSngb,
    COALESCE(apreensao.nome_orgao_detentor, bem.nome_orgao_detentor) AS detentor_orgao_nome,
    COALESCE(apreensao.orgao_detentor_id, bem.orgao_detentor_id) AS detentor_unidade_idSngb,
    COALESCE(apreensao.nome_orgao_detentor, bem.nome_orgao_detentor) AS detentor_unidade_nome

FROM snba.bem AS bem
LEFT JOIN snba.bem_especificacao AS bem_especificacao 
    ON bem_especificacao.bem_id = bem.id
LEFT JOIN snba.situacao AS situacao 
    ON situacao.id = bem.situacao_id
LEFT JOIN snba.subclasse AS subclasse 
    ON subclasse.id = bem.subclasse_id
LEFT JOIN snba.classe AS classe 
    ON classe.id = subclasse.classe_id
LEFT JOIN snba.categoria AS categoria 
    ON categoria.id = classe.categoria_id
LEFT JOIN snba.unidade_medida AS unidade_medida 
    ON unidade_medida.id = bem.unidade_medida_id
LEFT JOIN snba.apreensao AS apreensao 
    ON apreensao.id = bem.apreensao_id
WHERE bem.id = 340473; -- ou 530792























SELECT 
    bem.id,
    json_build_object(
        'id', bem.id,
        'versao', '0.3.0',
        'dataHoraAtualizacao', to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS.MS'),
        'dataHoraInclusaoSngb', bem.ctr_dth_inc,
        'dataHoraAtualizacaoSngb', bem.ctr_dth_atu,
        'dataRestricao', bem.data_restricao,

        -- Especificações
        'especificacoes', (
            SELECT json_build_object(
                'padrao', json_build_object(
                    'idSngb', padrao_especificacao.id,
                    'nome', padrao_especificacao.nome_padrao_especificacao
                ),
                'texto', bem.texto_especificacao_bem,
                'observacoes', bem.observacoes
            )
            FROM snba.padrao_especificacao AS padrao_especificacao
            WHERE padrao_especificacao.id = bem.padrao_especificacao_id
        ),

        -- Situação
        'situacao', (
            SELECT json_build_object(
                'padrao', json_build_object(
                    'idSngb', situacao.id,
                    'nome', situacao.nome_situacao
                )
            )
            FROM snba.situacao AS situacao
            WHERE situacao.id = bem.situacao_id
        ),

        -- Classificação
        'classificacao', (
            SELECT json_build_object(
                'categoria', json_build_object(
                    'idSngb', categoria.id,
                    'codigo', categoria.codigo_categoria,
                    'nome', categoria.nome_categoria
                ),
                'classe', json_build_object(
                    'idSngb', classe.id,
                    'codigo', classe.codigo_classe,
                    'nome', classe.nome_classe
                ),
                'subclasse', json_build_object(
                    'idSngb', subclasse.id,
                    'codigo', subclasse.codigo_subclasse,
                    'nome', subclasse.nome_subclasse
                )
            )
            FROM snba.subclasse AS subclasse
            JOIN snba.classe AS classe ON classe.id = subclasse.classe_id
            JOIN snba.categoria AS categoria ON categoria.id = classe.categoria_id
            WHERE subclasse.id = bem.subclasse_id
        ),

        -- Volume
        'volume', (
            SELECT json_build_object(
                'quantidade', bem.quantidade::INTEGER,
                'unidadeMedida', json_build_object(
                    'idSngb', unidade_medida.id,
                    'sigla', unidade_medida.sigla_unidade_medida,
                    'nome', unidade_medida.nome_unidade_medida
                )
            )
            FROM snba.unidade_medida AS unidade_medida
            WHERE bem.unidade_medida_id = unidade_medida.id
        ),

        -- Valores
        'valores', json_build_object(
            'estimado', bem.valor_estimado,
            'avaliado', bem.valor_avaliado
        ),

        -- Detentor
        'detentor', json_build_object(
            'orgao', json_build_object(
                'idSngb', COALESCE(
                    (SELECT apreensao.orgao_detentor_id 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.orgao_detentor_id
                ),
                'nome', COALESCE(
                    (SELECT apreensao.nome_orgao_detentor 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.nome_orgao_detentor
                )
            ),
            'unidade', json_build_object(
                'idSngb', COALESCE(
                    (SELECT apreensao.orgao_detentor_id 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.orgao_detentor_id
                ),
                'nome', COALESCE(
                    (SELECT apreensao.nome_orgao_detentor 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.nome_orgao_detentor
                )
            ),
            'localArmazenamento', json_build_object(
                'idSngb', 'acesso_armazenamento_id',
                'nome', 'acesso_nome_armazenamento',
                'tipo', json_build_object(
                    'idSngb', 'acesso_tipo_id',
                    'codigo', 'acesso_codigo',
                    'nome', 'acesso_nome'
                ),
                'endereco', json_build_object(
                    'idSngb', 'acesso_id',
                    'idEndereco', 'acesso_endereco_id',
                    'logradouro', 'acesso_logradouro',
                    'numero', 'acesso_numero',
                    'complemento', 'acesso_complemento',
                    'bairro', 'acesso_bairro',
                    'cep', 'acesso_cep',
                    'pais', 'acesso_pais',
                    'localidade', json_build_object(
                        'codigo', 'acesso_codigo_localidade',
                        'descricao', 'acesso_descricao'
                    ),
                    'uf', json_build_object(
                        'idSngb', 'acesso_municio_id',
                        'sigla', 'acesso_municipio_sigla',
                        'nome', 'acesso_municipio_nome'
                    ),
                    'pais', json_build_object(
                        'idSngb', 'acesso_pais_id',
                        'sigla', 'acesso_sigla',
                        'nome', 'acesso_nome_pais'
                    ),
                    'excluido', false
                ),
                'excluido', false
            )
        ),

        -- Apreensão
        'apreensao', (
            SELECT json_build_object(
                'dataApreensao', apreensao.data_apreensao,
                'local', apreensao.local_apreensao,
                'detalhesApreensao', json_build_object(
                    'idSngb', apreensao.id,
                    'dataHoraInclusao', apreensao.ctr_dth_inc,
                    'dataHoraAtualizacao', apreensao.ctr_dth_atu,
                    'tipo', json_build_object(
                        'idSngb', tipo_apreensao.id,
                        'nome', tipo_apreensao.nome_tipo
                    ),
                    'situacao', json_build_object(
                        'idSngb', 1,
                        'nome', 'Em Elaboração'
                    ),
                    'autoApreensao', json_build_object(
                        'emitido', apreensao.auto_apreensao_emitido,
                        'comprovanteEmitido', apreensao.comprovante_apreensao_emitido,
                        'responsavel', json_build_object(
                            'nome', apreensao.responsavel,
                            'documentosPrincipais', json_build_array(
                                json_build_object(
                                    'numero', apreensao.cpf_responsavel,
                                    'tipo', upper(split_part(apreensao.cpf_responsavel, '_', 1))
                                )
                            )
                        ),
                        'testemunhas', json_build_array(
                            json_build_object(
                                'nome', apreensao.responsavel,
                                'documentosPrincipais', json_build_array(
                                    json_build_object(
                                        'numero', apreensao.cpf_responsavel,
                                        'tipo', upper(split_part(apreensao.cpf_responsavel, '_', 1))
                                    )
                                )
                            )
                        )
                    ),
                    'idOrgaoJulgador', null,
                    'excluido', false
                )
            )
            FROM snba.apreensao AS apreensao
            LEFT JOIN snba.tipo_apreensao AS tipo_apreensao 
                ON tipo_apreensao.id = apreensao.tipo_apreensao_id
            WHERE apreensao.id = bem.apreensao_id
        ),

        -- Processos vinculados
        'processosVinculados', COALESCE(
            (
                SELECT json_agg(
                    json_build_object(
                        'idSngb', 'b.json_processos_vinculados_id',
                        'numeroProcesso', 'b.json_numeroProcesso',
                        'dataHoraInclusao', 'b.json_processo_data_de_inclusao',
                        'dataHoraAtualizacao', 'b.json_processo_data_de_atualizacao',
                        'nivelSigilo', 'b.json_processo_nivel_sigilo',--COALESCE('b.json_processo_nivel_sigilo', 0),
                        'status', 'b.json_processo_status_verificado',
                        'principal', 'b.json_processo_principal',
                        'orgaoJulgador', json_build_object(
                            'tribunal', json_build_object(
                                'segmento', 'JUSTICA_ESTADUAL',
                                'sigla', 'TJPB',
                                'nome', 'Tribunal de Justiça do Estado da Paraíba'
                            ),
                            'id', 'b.idOrgaoJulgador',
                            'nome', '4ª VARA CRIMINAL DE JOÃO PESSOA',
                            'jurisdicao', json_build_object(
                                'id', 1334,
                                'nome', 'JOAO PESSOA'
                            )
                        ),
                        'orgaoJulgadorColegiado', json_build_object(
                            'id', NULL,
                            'nome', NULL
                        ),
                        'excluido', false
                    )
                )
                FROM snba.apreensao AS apreensao
                LEFT JOIN snba.apreensao_processo_vinculado
                    ON apreensao_processo_vinculado.apreensao_id = apreensao.id
                WHERE apreensao.id = bem.apreensao_id
            ),
            '[]'::json
        )
    ) AS json_output
FROM snba.bem AS bem
WHERE bem.id = 530792;











SELECT 
    bem.id,
    json_build_object(
        'id', bem.id,
        'versao', '0.3.0',
        'dataHoraAtualizacao', to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS.MS'),
        'dataHoraInclusaoSngb', bem.ctr_dth_inc,
        'dataHoraAtualizacaoSngb', bem.ctr_dth_atu,
        'dataRestricao', bem.data_restricao,

        -- Especificações
        'especificacoes', (
            SELECT json_build_object(
                'padrao', json_build_object(
                    'idSngb', padrao_especificacao.id,
                    'nome', padrao_especificacao.nome_padrao_especificacao
                ),
                'texto', bem.texto_especificacao_bem,
                'observacoes', bem.observacoes
            )
            FROM snba.padrao_especificacao AS padrao_especificacao
            WHERE padrao_especificacao.id = bem.padrao_especificacao_id
        ),

        -- Situação
        'situacao', (
            SELECT json_build_object(
                'padrao', json_build_object(
                    'idSngb', situacao.id,
                    'nome', situacao.nome_situacao
                )
            )
            FROM snba.situacao AS situacao
            WHERE situacao.id = bem.situacao_id
        ),

        -- Classificação
        'classificacao', (
            SELECT json_build_object(
                'categoria', json_build_object(
                    'idSngb', categoria.id,
                    'codigo', categoria.codigo_categoria,
                    'nome', categoria.nome_categoria
                ),
                'classe', json_build_object(
                    'idSngb', classe.id,
                    'codigo', classe.codigo_classe,
                    'nome', classe.nome_classe
                ),
                'subclasse', json_build_object(
                    'idSngb', subclasse.id,
                    'codigo', subclasse.codigo_subclasse,
                    'nome', subclasse.nome_subclasse
                )
            )
            FROM snba.subclasse AS subclasse
            JOIN snba.classe AS classe ON classe.id = subclasse.classe_id
            JOIN snba.categoria AS categoria ON categoria.id = classe.categoria_id
            WHERE subclasse.id = bem.subclasse_id
        ),

        -- Volume
        'volume', (
            SELECT json_build_object(
                'quantidade', bem.quantidade::INTEGER,
                'unidadeMedida', json_build_object(
                    'idSngb', unidade_medida.id,
                    'sigla', unidade_medida.sigla_unidade_medida,
                    'nome', unidade_medida.nome_unidade_medida
                )
            )
            FROM snba.unidade_medida AS unidade_medida
            WHERE bem.unidade_medida_id = unidade_medida.id
        ),

        -- Valores
        'valores', json_build_object(
            'estimado', bem.valor_estimado,
            'avaliado', bem.valor_avaliado
        ),

        -- Detentor
        'detentor', json_build_object(
            'orgao', json_build_object(
                'idSngb', COALESCE(
                    (SELECT apreensao.orgao_detentor_id 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.orgao_detentor_id
                ),
                'nome', COALESCE(
                    (SELECT apreensao.nome_orgao_detentor 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.nome_orgao_detentor
                )
            ),
            'unidade', json_build_object(
                'idSngb', COALESCE(
                    (SELECT apreensao.orgao_detentor_id 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.orgao_detentor_id
                ),
                'nome', COALESCE(
                    (SELECT apreensao.nome_orgao_detentor 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.nome_orgao_detentor
                )
            ),
            'localArmazenamento', json_build_object(
                'idSngb', 'acesso_armazenamento_id',
                'nome', 'acesso_nome_armazenamento',
                'tipo', json_build_object(
                    'idSngb', 'acesso_tipo_id',
                    'codigo', 'acesso_codigo',
                    'nome', 'acesso_nome'
                ),
                'endereco', json_build_object(
                    'idSngb', 'acesso_id',
                    'idEndereco', 'acesso_endereco_id',
                    'logradouro', 'acesso_logradouro',
                    'numero', 'acesso_numero',
                    'complemento', 'acesso_complemento',
                    'bairro', 'acesso_bairro',
                    'cep', 'acesso_cep',
                    'pais', 'acesso_pais',
                    'localidade', json_build_object(
                        'codigo', 'acesso_codigo_localidade',
                        'descricao', 'acesso_descricao'
                    ),
                    'uf', json_build_object(
                        'idSngb', 'acesso_municio_id',
                        'sigla', 'acesso_municipio_sigla',
                        'nome', 'acesso_municipio_nome'
                    ),
                    'pais', json_build_object(
                        'idSngb', 'acesso_pais_id',
                        'sigla', 'acesso_sigla',
                        'nome', 'acesso_nome_pais'
                    ),
                    'excluido', false
                ),
                'excluido', false
            )
        ),

        -- Apreensão
        'apreensao', (
            SELECT json_build_object(
                'dataApreensao', apreensao.data_apreensao,
                'local', apreensao.local_apreensao,
                'detalhesApreensao', json_build_object(
                    'idSngb', apreensao.id,
                    'dataHoraInclusao', apreensao.ctr_dth_inc,
                    'dataHoraAtualizacao', apreensao.ctr_dth_atu,
                    'tipo', json_build_object(
                        'idSngb', tipo_apreensao.id,
                        'nome', tipo_apreensao.nome_tipo
                    ),
                    'situacao', json_build_object(
                        'idSngb', 1,
                        'nome', 'Em Elaboração'
                    ),
                    'autoApreensao', json_build_object(
                        'emitido', apreensao.auto_apreensao_emitido,
                        'comprovanteEmitido', apreensao.comprovante_apreensao_emitido,
                        'responsavel', json_build_object(
                            'nome', apreensao.responsavel,
                            'documentosPrincipais', json_build_array(
                                json_build_object(
                                    'numero', apreensao.cpf_responsavel,
                                    'tipo', upper(split_part(apreensao.cpf_responsavel, '_', 1))
                                )
                            )
                        ),
                        'testemunhas', json_build_array(
                            json_build_object(
                                'nome', apreensao.responsavel,
                                'documentosPrincipais', json_build_array(
                                    json_build_object(
                                        'numero', apreensao.cpf_responsavel,
                                        'tipo', upper(split_part(apreensao.cpf_responsavel, '_', 1))
                                    )
                                )
                            )
                        )
                    ),
                    'idOrgaoJulgador', null,
                    'excluido', false
                )
            )
            FROM snba.apreensao AS apreensao
            LEFT JOIN snba.tipo_apreensao AS tipo_apreensao 
                ON tipo_apreensao.id = apreensao.tipo_apreensao_id
            WHERE apreensao.id = bem.apreensao_id
        ),

        -- Processos vinculados usando LATERAL
        'processosVinculados', processos.processos_json
    ) AS json_output
FROM snba.bem AS bem
LEFT JOIN LATERAL (
    SELECT json_agg(
        json_build_object(
            'idSngb', 464894,
            'numeroProcesso', '0823054-44.2021.8.15.2002',
            'dataHoraInclusao', '2025-06-12 18:45:43.693',
            'dataHoraAtualizacao', '2025-06-12 18:45:43.693',
            'nivelSigilo', 0,
            'status', 'VERIFICADO',
            'principal', true,
            'orgaoJulgador', json_build_object(
                'tribunal', json_build_object(
                    'segmento', 'JUSTICA_ESTADUAL',
                    'sigla', 'TJPB',
                    'nome', 'Tribunal de Justiça do Estado da Paraíba'
                ),
                'id', 5570,
                'nome', '4ª VARA CRIMINAL DE JOÃO PESSOA',
                'jurisdicao', json_build_object(
                    'id', 1334,
                    'nome', 'JOAO PESSOA'
                )
            ),
            'orgaoJulgadorColegiado', json_build_object(
                'id', null,
                'nome', null
            ),
            'excluido', false
        )
    ) AS processos_json
) processos ON TRUE
WHERE bem.id = 530792;












SELECT 
	b.id AS json_id,
	b.ctr_dth_inc AS json_dataHoraInclusaoSngb,
	b.ctr_dth_atu AS json_dataHoraAtualizacaoSngb,
	b.data_restricao AS json_data_restricao,
	be.id AS json_especificacao_id,
	be.nome_atributo AS json_especificacao_nome,
	b.texto_especificacao_bem AS json_especificacao_texto,
	b.observacoes AS json_observacoes,
	s.id AS json_situacao_situacao_id,
	s.nome_situacao AS json_situacao_nome,
	c.id AS json_categoria_id,
	c.codigo_categoria AS json_categoria_codigo,
	c.nome_categoria AS json_categoria_nome,
	cla.id AS json_classe_id,
	cla.codigo_classe AS json_classe_codigo,
	cla.nome_classe AS json_classe_nome,
	subcla.id AS json_subclasse_id,
	subcla.codigo_subclasse AS json_subclasse_codigo,
	subcla.nome_subclasse AS json_subclasse_nome,
	b.quantidade AS volume_quantidade,
	b.unidade_medida_id AS volume_id,
	um.sigla_unidade_medida AS volume_sigla,
	um.nome_unidade_medida AS volume_nome,
	b.valor_estimado AS valores_estimado,
	b.valor_avaliado AS valores_avaliado,
	a.data_apreensao AS apreensao_data,
	a.local_apreensao AS apreensao_local,
	a.id AS apreensao_id,
	a.ctr_dth_inc as apreensao_datahorainclusao,
	a.ctr_dth_atu as apreensao_datahoraatualizacao,
	ta.id AS apreensao_tipo_id,
	ta.nome_tipo AS apreensao_nome,
	a.auto_apreensao_emitido AS emitido,
	a.comprovante_apreensao_emitido AS comprovante_emitido,
	a.responsavel AS nome_responsavel,
	a.cpf_responsavel AS cpf,
	bpv.orgao_julgador_id AS idOrgaoJulgador,
	bpv.id AS json_processos_vinculados_id,
	bpv.numero_processo AS json_numeroProcesso,
	bpv.ctr_dth_inc AS json_processo_data_de_inclusao,
	bpv.ctr_dth_atu AS json_processo_data_de_atualizacao,
	bpv.nivel_sigilo AS json_processo_nivel_sigilo,
	bpv.status AS json_processo_status_verificado,
	bpv.principal AS json_processo_principal
FROM snba.bem AS b
JOIN snba.bem_especificacao AS be ON b.padrao_especificacao_id = be.id
JOIN snba.situacao AS s ON s.id = b.situacao_id
JOIN snba.subclasse AS subcla ON subcla.id = b.subclasse_id 
JOIN snba.classe AS cla ON cla.id = subcla.classe_id
JOIN snba.categoria AS c ON c.id = cla.categoria_id
JOIN snba.unidade_medida AS um ON um.id = b.unidade_medida_id
JOIN snba.apreensao AS a ON a.id = b.apreensao_id 
JOIN snba.tipo_apreensao AS ta ON a.tipo_apreensao_id = ta.id 
JOIN snba.apreensao_processo_vinculado AS apv ON apv.apreensao_id = a.id 
JOIN snba.bem_processo_vinculado AS bpv ON bpv.bem_id = b.id
JOIN snba.padrao_especificacao AS pe 
	ON b.padrao_especificacao_id = pe.id 
	AND b.subclasse_id = subcla.id 
	AND pe.situacao_id = s.id 
WHERE b.id = 530792
GROUP BY 
	b.id, b.ctr_dth_inc, b.ctr_dth_atu, b.data_restricao,
	be.id, be.nome_atributo,
	b.texto_especificacao_bem, b.observacoes,
	s.id, s.nome_situacao,
	c.id, c.codigo_categoria, c.nome_categoria,
	cla.id, cla.codigo_classe, cla.nome_classe,
	subcla.id, subcla.codigo_subclasse, subcla.nome_subclasse,
	b.quantidade, b.unidade_medida_id, um.sigla_unidade_medida, um.nome_unidade_medida,
	b.valor_estimado, b.valor_avaliado,
	a.data_apreensao, a.local_apreensao, a.id, a.ctr_dth_inc, a.ctr_dth_atu,
	ta.id, ta.nome_tipo,
	a.auto_apreensao_emitido, a.comprovante_apreensao_emitido,
	a.responsavel, a.cpf_responsavel,
	bpv.orgao_julgador_id, bpv.id, bpv.numero_processo,
	bpv.ctr_dth_inc, bpv.ctr_dth_atu, bpv.nivel_sigilo,
	bpv.status, bpv.principal;



SELECT distinct *
FROM snba.bem AS b
--JOIN snba.bem_especificacao AS be ON b.padrao_especificacao_id = be.id
--JOIN snba.situacao AS s ON s.id = b.situacao_id
--JOIN snba.subclasse AS subcla ON subcla.id = b.subclasse_id 
--JOIN snba.classe AS cla ON cla.id = subcla.classe_id
--JOIN snba.categoria AS c ON c.id = cla.categoria_id
--JOIN snba.unidade_medida AS um ON um.id = b.unidade_medida_id
JOIN snba.apreensao AS a ON a.id = b.apreensao_id 
--JOIN snba.tipo_apreensao AS ta ON a.tipo_apreensao_id = ta.id 
JOIN snba.apreensao_processo_vinculado AS apv ON apv.apreensao_id = a.id 
JOIN snba.bem_processo_vinculado AS bpv ON bpv.bem_id = b.id
WHERE b.id = 530792



SELECT 
    b.*,
    --a.*,
    --json_agg(DISTINCT apv.*) AS processos_apreensao,
    json_agg(DISTINCT bpv.*) AS processos_bem
FROM snba.bem AS b
JOIN snba.apreensao AS a ON a.id = b.apreensao_id
LEFT JOIN snba.apreensao_processo_vinculado AS apv 
       ON apv.apreensao_id = a.id
LEFT JOIN snba.bem_processo_vinculado AS bpv 
       ON bpv.bem_id = b.id
WHERE b.id = 530792
GROUP BY b.id


SELECT DISTINCT bpv.*
    --b.*,
    --json_agg(DISTINCT bpv.*) AS processos_bem
FROM snba.bem AS b
JOIN snba.apreensao AS a ON a.id = b.apreensao_id
LEFT JOIN snba.apreensao_processo_vinculado AS apv 
       ON apv.apreensao_id = a.id
LEFT JOIN snba.bem_processo_vinculado AS bpv 
       ON bpv.bem_id = b.id
WHERE b.id = 530792
--GROUP BY b.id



SELECT DISTINCT bpv.*,a.*
    --b.*,
    --json_agg(DISTINCT bpv.*) AS processos_bem
FROM snba.bem AS b
JOIN snba.apreensao AS a ON a.id = b.apreensao_id
LEFT JOIN snba.apreensao_processo_vinculado AS apv 
       ON apv.apreensao_id = a.id
LEFT JOIN snba.bem_processo_vinculado AS bpv 
       ON bpv.bem_id = b.id
WHERE b.id = 530792






SELECT 
    bem.id,
    json_build_object(
        'id', bem.id,
        'versao', '0.3.0',
        'dataHoraAtualizacao', to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS.MS'),
        'dataHoraInclusaoSngb', bem.ctr_dth_inc,
        'dataHoraAtualizacaoSngb', bem.ctr_dth_atu,
        'dataRestricao', bem.data_restricao,

        -- Especificações
        'especificacoes', (
            SELECT json_build_object(
                'padrao', json_build_object(
                    'idSngb', padrao_especificacao.id,
                    'nome', padrao_especificacao.nome_padrao_especificacao
                ),
                'texto', bem.texto_especificacao_bem,
                'observacoes', bem.observacoes
            )
            FROM snba.padrao_especificacao AS padrao_especificacao
            WHERE padrao_especificacao.id = bem.padrao_especificacao_id
        ),

        -- Situação
        'situacao', (
            SELECT json_build_object(
                'padrao', json_build_object(
                    'idSngb', situacao.id,
                    'nome', situacao.nome_situacao
                )
            )
            FROM snba.situacao AS situacao
            WHERE situacao.id = bem.situacao_id
        ),

        -- Classificação
        'classificacao', (
            SELECT json_build_object(
                'categoria', json_build_object(
                    'idSngb', categoria.id,
                    'codigo', categoria.codigo_categoria,
                    'nome', categoria.nome_categoria
                ),
                'classe', json_build_object(
                    'idSngb', classe.id,
                    'codigo', classe.codigo_classe,
                    'nome', classe.nome_classe
                ),
                'subclasse', json_build_object(
                    'idSngb', subclasse.id,
                    'codigo', subclasse.codigo_subclasse,
                    'nome', subclasse.nome_subclasse
                )
            )
            FROM snba.subclasse AS subclasse
            JOIN snba.classe AS classe ON classe.id = subclasse.classe_id
            JOIN snba.categoria AS categoria ON categoria.id = classe.categoria_id
            WHERE subclasse.id = bem.subclasse_id
        ),

        -- Volume
        'volume', (
            SELECT json_build_object(
                'quantidade', bem.quantidade::INTEGER,
                'unidadeMedida', json_build_object(
                    'idSngb', unidade_medida.id,
                    'sigla', unidade_medida.sigla_unidade_medida,
                    'nome', unidade_medida.nome_unidade_medida
                )
            )
            FROM snba.unidade_medida AS unidade_medida
            WHERE bem.unidade_medida_id = unidade_medida.id
        ),

        -- Valores
        'valores', json_build_object(
            'estimado', bem.valor_estimado,
            'avaliado', bem.valor_avaliado
        ),

        -- Detentor
        'detentor', json_build_object(
            'orgao', json_build_object(
                'idSngb', COALESCE(
                    (SELECT apreensao.orgao_detentor_id 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.orgao_detentor_id
                ),
                'nome', COALESCE(
                    (SELECT apreensao.nome_orgao_detentor 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.nome_orgao_detentor
                )
            ),
            'unidade', json_build_object(
                'idSngb', COALESCE(
                    (SELECT apreensao.orgao_detentor_id 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.orgao_detentor_id
                ),
                'nome', COALESCE(
                    (SELECT apreensao.nome_orgao_detentor 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.nome_orgao_detentor
                )
            ),
            'localArmazenamento', json_build_object(
                'idSngb', 'acesso_armazenamento_id',
                'nome', 'acesso_nome_armazenamento',
                'tipo', json_build_object(
                    'idSngb', 'acesso_tipo_id',
                    'codigo', 'acesso_codigo',
                    'nome', 'acesso_nome'
                ),
                'endereco', json_build_object(
                    'idSngb', 'acesso_id',
                    'idEndereco', 'acesso_endereco_id',
                    'logradouro', 'acesso_logradouro',
                    'numero', 'acesso_numero',
                    'complemento', 'acesso_complemento',
                    'bairro', 'acesso_bairro',
                    'cep', 'acesso_cep',
                    'pais', 'acesso_pais',
                    'localidade', json_build_object(
                        'codigo', 'acesso_codigo_localidade',
                        'descricao', 'acesso_descricao'
                    ),
                    'uf', json_build_object(
                        'idSngb', 'acesso_municio_id',
                        'sigla', 'acesso_municipio_sigla',
                        'nome', 'acesso_municipio_nome'
                    ),
                    'pais', json_build_object(
                        'idSngb', 'acesso_pais_id',
                        'sigla', 'acesso_sigla',
                        'nome', 'acesso_nome_pais'
                    ),
                    'excluido', false
                ),
                'excluido', false
            )
        ),

        -- Apreensão
        'apreensao', (
            SELECT json_build_object(
                'dataApreensao', apreensao.data_apreensao,
                'local', apreensao.local_apreensao,
                'detalhesApreensao', json_build_object(
                    'idSngb', apreensao.id,
                    'dataHoraInclusao', apreensao.ctr_dth_inc,
                    'dataHoraAtualizacao', apreensao.ctr_dth_atu,
                    'tipo', json_build_object(
                        'idSngb', tipo_apreensao.id,
                        'nome', tipo_apreensao.nome_tipo
                    ),
                    'situacao', json_build_object(
                        'idSngb', 1,
                        'nome', 'Em Elaboração'
                    ),
                    'autoApreensao', json_build_object(
                        'emitido', apreensao.auto_apreensao_emitido,
                        'comprovanteEmitido', apreensao.comprovante_apreensao_emitido,
                        'responsavel', json_build_object(
                            'nome', apreensao.responsavel,
                            'documentosPrincipais', json_build_array(
                                json_build_object(
                                    'numero', apreensao.cpf_responsavel,
                                    'tipo', upper(split_part(apreensao.cpf_responsavel, '_', 1))
                                )
                            )
                        ),
                        'testemunhas', json_build_array(
                            json_build_object(
                                'nome', apreensao.responsavel,
                                'documentosPrincipais', json_build_array(
                                    json_build_object(
                                        'numero', apreensao.cpf_responsavel,
                                        'tipo', upper(split_part(apreensao.cpf_responsavel, '_', 1))
                                    )
                                )
                            )
                        )
                    ),
                    'idOrgaoJulgador', null,
                    'excluido', false
                )
            )
            FROM snba.apreensao AS apreensao
            LEFT JOIN snba.tipo_apreensao AS tipo_apreensao 
                ON tipo_apreensao.id = apreensao.tipo_apreensao_id
            WHERE apreensao.id = bem.apreensao_id
        ),

        -- Processos vinculados (DISTINCT aplicado)
        'processosVinculados', COALESCE(
            (
                SELECT json_agg(
                    json_build_object(
                        'idSngb', bem_processo_vinculado_id, --bem_processo_vinculado.id,
                        'numeroProcesso', bem_processo_vinculado_numero_processo, --bem_processo_vinculado.numero_processo,
                        'dataHoraInclusao', to_char(bem_processo_vinculado_ctr_dth_inc, 'YYYY-MM-DD HH24:MI:SS.MS'),
                        'dataHoraAtualizacao', to_char(bem_processo_vinculado_ctr_dth_atu, 'YYYY-MM-DD HH24:MI:SS.MS'),
                        'nivelSigilo', COALESCE(bem_processo_vinculado_nivel_sigilo, 0),
                        'status', bem_processo_vinculado_status, --bem_processo_vinculado.status,
                        'principal', bem_processo_vinculado_principal, --bem_processo_vinculado.principal,
                        'orgaoJulgador', json_build_object(
                            'tribunal', json_build_object(
                                'segmento', 'JUSTICA_ESTADUAL',
                                'sigla', 'TJPB',
                                'nome', 'Tribunal de Justiça do Estado da Paraíba'
                            ),
                            'id', 'bem_processo_vinculado.id_orgao_julgador',
                            'nome', '4ª VARA CRIMINAL DE JOÃO PESSOA',
                            'jurisdicao', json_build_object(
                                'id', apreensao_orgao_detentor_id, ---1334,
                                'nome', apreensao_nome_orgao_detentor --'JOAO PESSOA'
                            )
                        ),
                        'orgaoJulgadorColegiado', json_build_object(
                            'id', NULL,
                            'nome', NULL
                        ),
                        'excluido', false
                    )
                )
                FROM (
						SELECT DISTINCT 
						bem_processo_vinculado.id as bem_processo_vinculado_id,
						bem_processo_vinculado.numero_processo as bem_processo_vinculado_numero_processo,
						bem_processo_vinculado.ctr_dth_inc as bem_processo_vinculado_ctr_dth_inc,
						bem_processo_vinculado.ctr_dth_atu as bem_processo_vinculado_ctr_dth_atu,
						bem_processo_vinculado.nivel_sigilo as bem_processo_vinculado_nivel_sigilo,
						bem_processo_vinculado.status as bem_processo_vinculado_status,
						bem_processo_vinculado.principal as bem_processo_vinculado_principal,
						apreensao.orgao_detentor_id as apreensao_orgao_detentor_id, 
						apreensao.nome_orgao_detentor as apreensao_nome_orgao_detentor
						FROM snba.apreensao as apreensao
						JOIN snba.apreensao_processo_vinculado AS apreensao_processo_vinculado
						    ON apreensao_processo_vinculado.apreensao_id = apreensao.id
						JOIN snba.bem_processo_vinculado as bem_processo_vinculado
						    ON bem_processo_vinculado.bem_id = bem.id
                ) AS processosVinculados
            ), '[]'
        ),

        'excluido', false
    ) AS json_output
FROM snba.bem AS bem
WHERE bem.id = 530792;


SELECT DISTINCT 
bem_processo_vinculado.id as bem_processo_vinculado_id,
bem_processo_vinculado.numero_processo as bem_processo_vinculado_numero_processo,
bem_processo_vinculado.ctr_dth_inc as bem_processo_vinculado_ctr_dth_inc,
bem_processo_vinculado.ctr_dth_atu as bem_processo_vinculado_ctr_dth_atu,
bem_processo_vinculado.nivel_sigilo as bem_processo_vinculado_nivel_sigilo,
bem_processo_vinculado.status as bem_processo_vinculado_status,
bem_processo_vinculado.principal as bem_processo_vinculado_principal,
apreensao.orgao_detentor_id as apreensao_orgao_detentor_id, 
apreensao.nome_orgao_detentor as apreensao_nome_orgao_detentor, 
bem_processo_vinculado.numero_processo as bem_processo_vinculado_numero_processo
FROM snba.bem AS bem
JOIN snba.apreensao AS apreensao 
    ON apreensao.id = bem.apreensao_id
JOIN snba.apreensao_processo_vinculado AS apv 
    ON apv.apreensao_id = apreensao.id
JOIN snba.bem_processo_vinculado
    ON bem_processo_vinculado.bem_id = bem.id
WHERE bem.id = 530792;


SELECT DISTINCT bpv.*,a.*
    --b.*,
    --json_agg(DISTINCT bpv.*) AS processos_bem
FROM snba.bem AS b
JOIN snba.apreensao AS a ON a.id = b.apreensao_id
LEFT JOIN snba.apreensao_processo_vinculado AS apv 
       ON apv.apreensao_id = a.id
LEFT JOIN snba.bem_processo_vinculado AS bpv 
       ON bpv.bem_id = b.id
WHERE b.id = 530792






SELECT DISTINCT 
    bem_processo_vinculado.id as bem_processo_vinculado_id,
    bem_processo_vinculado.numero_processo as bem_processo_vinculado_numero_processo,
    bem_processo_vinculado.ctr_dth_inc as bem_processo_vinculado_ctr_dth_inc,
    bem_processo_vinculado.ctr_dth_atu as bem_processo_vinculado_ctr_dth_atu,
    bem_processo_vinculado.nivel_sigilo as bem_processo_vinculado_nivel_sigilo,
    bem_processo_vinculado.status as bem_processo_vinculado_status,
    bem_processo_vinculado.principal as bem_processo_vinculado_principal,
    apreensao.orgao_detentor_id as apreensao_orgao_detentor_id, 
    apreensao.nome_orgao_detentor as apreensao_nome_orgao_detentor
FROM snba.bem AS bem
JOIN snba.apreensao as apreensao
    ON apreensao.id = bem.apreensao_id
JOIN snba.apreensao_processo_vinculado AS apreensao_processo_vinculado
    ON apreensao_processo_vinculado.apreensao_id = apreensao.id
JOIN snba.bem_processo_vinculado as bem_processo_vinculado
    ON bem_processo_vinculado.bem_id = bem.id
WHERE bem.id = 530792;















SELECT 
    bem.id,
    json_build_object(
        'id', bem.id,
        'versao', '0.3.0',
        'dataHoraAtualizacao', to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS.MS'),
        'dataHoraInclusaoSngb', bem.ctr_dth_inc,
        'dataHoraAtualizacaoSngb', bem.ctr_dth_atu,
        'dataRestricao', bem.data_restricao,

        -- Especificações
        'especificacoes', (
            SELECT json_build_object(
                'padrao', json_build_object(
                    'idSngb', padrao_especificacao.id,
                    'nome', padrao_especificacao.nome_padrao_especificacao
                ),
                'texto', bem.texto_especificacao_bem,
                'observacoes', bem.observacoes
            )
            FROM snba.padrao_especificacao AS padrao_especificacao
            WHERE padrao_especificacao.id = bem.padrao_especificacao_id
        ),

        -- Situação
        'situacao', (
            SELECT json_build_object(
                'padrao', json_build_object(
                    'idSngb', situacao.id,
                    'nome', situacao.nome_situacao
                )
            )
            FROM snba.situacao AS situacao
            WHERE situacao.id = bem.situacao_id
        ),

        -- Classificação
        'classificacao', (
            SELECT json_build_object(
                'categoria', json_build_object(
                    'idSngb', categoria.id,
                    'codigo', categoria.codigo_categoria,
                    'nome', categoria.nome_categoria
                ),
                'classe', json_build_object(
                    'idSngb', classe.id,
                    'codigo', classe.codigo_classe,
                    'nome', classe.nome_classe
                ),
                'subclasse', json_build_object(
                    'idSngb', subclasse.id,
                    'codigo', subclasse.codigo_subclasse,
                    'nome', subclasse.nome_subclasse
                )
            )
            FROM snba.subclasse AS subclasse
            JOIN snba.classe AS classe ON classe.id = subclasse.classe_id
            JOIN snba.categoria AS categoria ON categoria.id = classe.categoria_id
            WHERE subclasse.id = bem.subclasse_id
        ),

        -- Volume
        'volume', (
            SELECT json_build_object(
                'quantidade', bem.quantidade::INTEGER,
                'unidadeMedida', json_build_object(
                    'idSngb', unidade_medida.id,
                    'sigla', unidade_medida.sigla_unidade_medida,
                    'nome', unidade_medida.nome_unidade_medida
                )
            )
            FROM snba.unidade_medida AS unidade_medida
            WHERE bem.unidade_medida_id = unidade_medida.id
        ),

        -- Valores
        'valores', json_build_object(
            'estimado', bem.valor_estimado,
            'avaliado', bem.valor_avaliado
        ),

        -- Detentor
        'detentor', json_build_object(
            'orgao', json_build_object(
                'idSngb', COALESCE(
                    (SELECT apreensao.orgao_detentor_id 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.orgao_detentor_id
                ),
                'nome', COALESCE(
                    (SELECT apreensao.nome_orgao_detentor 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.nome_orgao_detentor
                )
            ),
            'unidade', json_build_object(
                'idSngb', COALESCE(
                    (SELECT apreensao.orgao_detentor_id 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.orgao_detentor_id
                ),
                'nome', COALESCE(
                    (SELECT apreensao.nome_orgao_detentor 
                     FROM snba.apreensao AS apreensao 
                     WHERE apreensao.id = bem.apreensao_id),
                    bem.nome_orgao_detentor
                )
            ),
            'localArmazenamento', json_build_object(
                'idSngb', 'acesso_armazenamento_id',
                'nome', 'acesso_nome_armazenamento',
                'tipo', json_build_object(
                    'idSngb', 'acesso_tipo_id',
                    'codigo', 'acesso_codigo',
                    'nome', 'acesso_nome'
                ),
                'endereco', json_build_object(
                    'idSngb', 'acesso_id',
                    'idEndereco', 'acesso_endereco_id',
                    'logradouro', 'acesso_logradouro',
                    'numero', 'acesso_numero',
                    'complemento', 'acesso_complemento',
                    'bairro', 'acesso_bairro',
                    'cep', 'acesso_cep',
                    'pais', 'acesso_pais',
                    'localidade', json_build_object(
                        'codigo', 'acesso_codigo_localidade',
                        'descricao', 'acesso_descricao'
                    ),
                    'uf', json_build_object(
                        'idSngb', 'acesso_municio_id',
                        'sigla', 'acesso_municipio_sigla',
                        'nome', 'acesso_municipio_nome'
                    ),
                    'pais', json_build_object(
                        'idSngb', 'acesso_pais_id',
                        'sigla', 'acesso_sigla',
                        'nome', 'acesso_nome_pais'
                    ),
                    'excluido', false
                ),
                'excluido', false
            )
        ),

        -- Apreensão
        'apreensao', (
            SELECT json_build_object(
                'dataApreensao', apreensao.data_apreensao,
                'local', apreensao.local_apreensao,
                'detalhesApreensao', json_build_object(
                    'idSngb', apreensao.id,
                    'dataHoraInclusao', apreensao.ctr_dth_inc,
                    'dataHoraAtualizacao', apreensao.ctr_dth_atu,
                    'tipo', json_build_object(
                        'idSngb', tipo_apreensao.id,
                        'nome', tipo_apreensao.nome_tipo
                    ),
                    'situacao', json_build_object(
                        'idSngb', 1,
                        'nome', 'Em Elaboração'
                    ),
                    'autoApreensao', json_build_object(
                        'emitido', apreensao.auto_apreensao_emitido,
                        'comprovanteEmitido', apreensao.comprovante_apreensao_emitido,
                        'responsavel', json_build_object(
                            'nome', apreensao.responsavel,
                            'documentosPrincipais', json_build_array(
                                json_build_object(
                                    'numero', apreensao.cpf_responsavel,
                                    'tipo', upper(split_part(apreensao.cpf_responsavel, '_', 1))
                                )
                            )
                        ),
                        'testemunhas', json_build_array(
                            json_build_object(
                                'nome', apreensao.responsavel,
                                'documentosPrincipais', json_build_array(
                                    json_build_object(
                                        'numero', apreensao.cpf_responsavel,
                                        'tipo', upper(split_part(apreensao.cpf_responsavel, '_', 1))
                                    )
                                )
                            )
                        )
                    ),
                    'idOrgaoJulgador', null,
                    'excluido', false
                )
            )
            FROM snba.apreensao AS apreensao
            LEFT JOIN snba.tipo_apreensao AS tipo_apreensao 
                ON tipo_apreensao.id = apreensao.tipo_apreensao_id
            WHERE apreensao.id = bem.apreensao_id
        ),

        -- Processos vinculados (DISTINCT aplicado)
        'processosVinculados', COALESCE(
            (
				SELECT json_agg(
				    json_build_object(
				        'idSngb', bem_processo_vinculado_id,
				        'numeroProcesso', bem_processo_vinculado_numero_processo,
				        'dataHoraInclusao', to_char(bem_processo_vinculado_ctr_dth_inc, 'YYYY-MM-DD HH24:MI:SS.MS'),
				        'dataHoraAtualizacao', to_char(bem_processo_vinculado_ctr_dth_atu, 'YYYY-MM-DD HH24:MI:SS.MS'),
				        'nivelSigilo', COALESCE(bem_processo_vinculado_nivel_sigilo, 0),
				        'status', bem_processo_vinculado_status,
				        'principal', bem_processo_vinculado_principal,
				        'orgaoJulgador', json_build_object(
				            'tribunal', json_build_object(
				                'segmento', 'JUSTICA_ESTADUAL',
				                'sigla', 'TJPB',
				                'nome', 'Tribunal de Justiça do Estado da Paraíba'
				            ),
				            'id', NULL,
				            'nome', NULL,
				            'jurisdicao', json_build_object(
				                'id', apreensao_orgao_detentor_id,
				                'nome', apreensao_nome_orgao_detentor
				            )
				        ),
				        'orgaoJulgadorColegiado', json_build_object(
				            'id', NULL,
				            'nome', NULL
				        ),
				        'excluido', false
				    )
				)
				FROM (
				    SELECT DISTINCT 
				        bem_processo_vinculado.id AS bem_processo_vinculado_id,
				        bem_processo_vinculado.numero_processo AS bem_processo_vinculado_numero_processo,
				        bem_processo_vinculado.ctr_dth_inc AS bem_processo_vinculado_ctr_dth_inc,
				        bem_processo_vinculado.ctr_dth_atu AS bem_processo_vinculado_ctr_dth_atu,
				        bem_processo_vinculado.nivel_sigilo AS bem_processo_vinculado_nivel_sigilo,
				        bem_processo_vinculado.status AS bem_processo_vinculado_status,
				        bem_processo_vinculado.principal AS bem_processo_vinculado_principal,
				        apreensao.orgao_detentor_id AS apreensao_orgao_detentor_id, 
				        apreensao.nome_orgao_detentor AS apreensao_nome_orgao_detentor
				    FROM snba.bem_processo_vinculado
				    LEFT JOIN snba.apreensao 
				        ON apreensao.id = (SELECT bem.apreensao_id FROM snba.bem AS bem WHERE bem.id = bem_processo_vinculado.bem_id)
				    WHERE bem_processo_vinculado.bem_id = 530792
				) AS processosVinculados
            ), '[]'
        ),

        'excluido', false
    ) AS json_output
FROM snba.bem AS bem
WHERE bem.id = 530792;










SQL Error [42601]: ERROR: syntax error at or near "AS"
  Position: 9313

Error position: line: 2661 pos: 9312



SQL Error [42601]: ERROR: syntax error at or near "AS"
  Position: 9689

Error position: line: 2676 pos: 9688


SELECT 
    bem.id,
    json_build_object(
        'id', bem.id,
        'versao', '0.3.0',
        'dataHoraAtualizacao', to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS.MS'),
        'dataHoraInclusaoSngb', bem.ctr_dth_inc,
        'dataHoraAtualizacaoSngb', bem.ctr_dth_atu,
        'dataRestricao', bem.data_restricao,

        -- Especificações
        'especificacoes', (
            SELECT json_build_object(
                'padrao', json_build_object(
                    'idSngb', padrao_especificacao.id,
                    'nome', padrao_especificacao.nome_padrao_especificacao
                ),
                'texto', bem.texto_especificacao_bem,
                'observacoes', bem.observacoes
            )
            FROM snba.padrao_especificacao AS padrao_especificacao
            WHERE padrao_especificacao.id = bem.padrao_especificacao_id
        ),

        -- Situação
        'situacao', (
            SELECT json_build_object(
                'padrao', json_build_object(
                    'idSngb', situacao.id,
                    'nome', situacao.nome_situacao
                )
            )
            FROM snba.situacao AS situacao
            WHERE situacao.id = bem.situacao_id
        ),

        -- Classificação
        'classificacao', (
            SELECT json_build_object(
                'categoria', json_build_object(
                    'idSngb', categoria.id,
                    'codigo', categoria.codigo_categoria,
                    'nome', categoria.nome_categoria
                ),
                'classe', json_build_object(
                    'idSngb', classe.id,
                    'codigo', classe.codigo_classe,
                    'nome', classe.nome_classe
                ),
                'subclasse', json_build_object(
                    'idSngb', subclasse.id,
                    'codigo', subclasse.codigo_subclasse,
                    'nome', subclasse.nome_subclasse
                )
            )
            FROM snba.subclasse AS subclasse
            JOIN snba.classe AS classe ON classe.id = subclasse.classe_id
            JOIN snba.categoria AS categoria ON categoria.id = classe.categoria_id
            WHERE subclasse.id = bem.subclasse_id
        ),

        -- Volume
        'volume', (
            SELECT json_build_object(
                'quantidade', bem.quantidade::INTEGER,
                'unidadeMedida', json_build_object(
                    'idSngb', unidade_medida.id,
                    'sigla', unidade_medida.sigla_unidade_medida,
                    'nome', unidade_medida.nome_unidade_medida
                )
            )
            FROM snba.unidade_medida AS unidade_medida
            WHERE bem.unidade_medida_id = unidade_medida.id
        ),

        -- Valores
        'valores', json_build_object(
            'estimado', bem.valor_estimado,
            'avaliado', bem.valor_avaliado
        ),
        -- Detentor
        'detentor', json_build_object(
            'orgao', json_build_object(
                'idSngb', apreensao.orgao_detentor_id,
                'nome', apreensao.nome_orgao_detentor
            ),
            'unidade', json_build_object(
                'idSngb', bem.unidade_detentora_id,
                'nome', bem.nome_orgao_detentor
            )
        ),

        -- Apreensão
        'apreensao', (
            SELECT json_build_object(
                'dataApreensao', apreensao.data_apreensao,
                'local', apreensao.local_apreensao,
                'detalhesApreensao', json_build_object(
                    'idSngb', apreensao.id,
                    'dataHoraInclusao', apreensao.ctr_dth_inc,
                    'dataHoraAtualizacao', apreensao.ctr_dth_atu,
                    'tipo', json_build_object(
                        'idSngb', tipo_apreensao.id,
                        'nome', tipo_apreensao.nome_tipo
                    ),
                    'situacao', json_build_object(
                        'idSngb', 1,
                        'nome', 'Em Elaboração'
                    ),
                    'autoApreensao', json_build_object(
                        'emitido', apreensao.auto_apreensao_emitido,
                        'comprovanteEmitido', apreensao.comprovante_apreensao_emitido,
                        'responsavel', json_build_object(
                            'nome', apreensao.responsavel,
                            'documentosPrincipais', json_build_array(
                                json_build_object(
                                    'numero', apreensao.cpf_responsavel,
                                    'tipo', upper(split_part('cpf_responsavel', '_', 1))
                                )
                            )
                        ),
                        'testemunhas', json_build_array(
                            json_build_object(
                                'nome', apreensao.responsavel,
                                'documentosPrincipais', json_build_array(
                                    json_build_object(
                                        'numero', apreensao.cpf_responsavel,
                                        'tipo', upper(split_part('cpf_responsavel', '_', 1))
                                    )
                                )
                            )
                        )
                    ),
                    'idOrgaoJulgador', NULL,
                    'orgaoDetentor', json_build_object(
                        'idSngb', apreensao.orgao_detentor_id,
                        'nome', apreensao.nome_orgao_detentor
                    ),
                    'excluido', false
                )
            )
            FROM snba.apreensao AS apreensao
            LEFT JOIN snba.tipo_apreensao AS tipo_apreensao 
                ON tipo_apreensao.id = apreensao.tipo_apreensao_id
            WHERE apreensao.id = bem.apreensao_id
        ),

        -- Processos vinculados
        'processosVinculados', COALESCE(
            (
                SELECT json_agg(
                    json_build_object(
                        'idSngb', bem_processo_vinculado_id,
                        'numeroProcesso', bem_processo_vinculado_numero_processo,
                        'dataHoraInclusao', to_char(bem_processo_vinculado_ctr_dth_inc, 'YYYY-MM-DD HH24:MI:SS.MS'),
                        'dataHoraAtualizacao', to_char(bem_processo_vinculado_ctr_dth_atu, 'YYYY-MM-DD HH24:MI:SS.MS'),
                        'nivelSigilo', COALESCE(bem_processo_vinculado_nivel_sigilo, 0),
                        'status', bem_processo_vinculado_status,
                        'principal', bem_processo_vinculado_principal,
                        'orgaoJulgador', json_build_object(
                            'tribunal', json_build_object(
                                'segmento', 'JUSTICA_ESTADUAL',
                                'sigla', 'TJPB',
                                'nome', 'Tribunal de Justiça do Estado da Paraíba'
                            ),
                            'id', NULL,
                            'nome', NULL,
                            'jurisdicao', NULL
                        ),
                        'orgaoJulgadorColegiado', json_build_object(
                            'id', NULL,
                            'nome', NULL
                        ),
                        'excluido', false
                    )
                )
                FROM (
                    SELECT DISTINCT 
                        bem_processo_vinculado.id AS bem_processo_vinculado_id,
                        bem_processo_vinculado.numero_processo AS bem_processo_vinculado_numero_processo,
                        bem_processo_vinculado.ctr_dth_inc AS bem_processo_vinculado_ctr_dth_inc,
                        bem_processo_vinculado.ctr_dth_atu AS bem_processo_vinculado_ctr_dth_atu,
                        bem_processo_vinculado.nivel_sigilo AS bem_processo_vinculado_nivel_sigilo,
                        bem_processo_vinculado.status AS bem_processo_vinculado_status,
                        bem_processo_vinculado.principal AS bem_processo_vinculado_principal
                    FROM snba.bem_processo_vinculado
                    WHERE bem_processo_vinculado.bem_id = 530792
                ) AS processosVinculados
            ), '[]'
        ),

        'excluido', false
    ) AS json_output
FROM snba.bem AS bem
LEFT JOIN snba.apreensao AS apreensao
       ON apreensao.id = bem.apreensao_id
WHERE bem.id = 530792;


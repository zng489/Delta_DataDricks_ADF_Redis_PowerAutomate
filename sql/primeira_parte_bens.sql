    SELECT 
        json_build_object(
            'id', bem.id,
            'versao', '0.3.0',
            'dataHoraAtualizacao', to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS.MS'),
            'dataHoraInclusaoSngb', COALESCE(bem.ctr_dth_inc, null),
            'dataHoraAtualizacaoSngb', COALESCE(bem.ctr_dth_atu, null),
            'dataRestricao', COALESCE(bem.data_restricao, null),

            -- Especificações
            'especificacoes', json_build_object(
                'padrao', json_build_object(
                    'idSngb', COALESCE(padrao_especificacao.id, null),
                    'nome', COALESCE(padrao_especificacao.nome_padrao_especificacao, null)
                ),
                'texto', COALESCE(bem.texto_especificacao_bem, null),
                'observacoes', COALESCE(bem.observacoes, null)
            ),

            -- Situação
            'situacao', json_build_object(
                'padrao', json_build_object(
                    'idSngb', COALESCE(situacao.id, null),
                    'nome', COALESCE(situacao.nome_situacao, null)
                )
            ),

            -- Classificação
            'classificacao', json_build_object(
                'categoria', json_build_object(
                    'idSngb', COALESCE(categoria.id, null),
                    'codigo', COALESCE(categoria.codigo_categoria, null),
                    'nome', COALESCE(categoria.nome_categoria, null)
                ),
                'classe', json_build_object(
                    'idSngb', COALESCE(classe.id, null),
                    'codigo', COALESCE(classe.codigo_classe, null),
                    'nome', COALESCE(classe.nome_classe, null)
                ),
                'subclasse', json_build_object(
                    'idSngb', COALESCE(subclasse.id, null),
                    'codigo', COALESCE(subclasse.codigo_subclasse, null),
                    'nome', COALESCE(subclasse.nome_subclasse, null)
                )
            ),

            -- Volume
            'volume', json_build_object(
                'quantidade', COALESCE(bem.quantidade, null)::INTEGER,
                'unidadeMedida', json_build_object(
                    'idSngb', COALESCE(unidade_medida.id, null),
                    'sigla', COALESCE(unidade_medida.sigla_unidade_medida, null),
                    'nome', COALESCE(unidade_medida.nome_unidade_medida, null)
                )
            ),

            -- Valores
            'valores', json_build_object(
                'estimado', COALESCE(bem.valor_estimado, null),
                'avaliado', COALESCE(bem.valor_avaliado, null)
            ),
            
            -- Detentor
            'detentor', json_build_object(
                'orgao', json_build_object(
                    'idSngb', COALESCE(apreensao.orgao_detentor_id, null),
                    'nome', COALESCE(apreensao.nome_orgao_detentor, null)
                ),
                'unidade', json_build_object(
                    'idSngb', COALESCE(bem.unidade_detentora_id, null),
                    'nome', COALESCE(bem.nome_orgao_detentor, null)
                )
            ),

            -- Apreensão
            'apreensao', json_build_object(
                'dataApreensao', COALESCE(apreensao.data_apreensao, null),
                'local', COALESCE(apreensao.local_apreensao, null),
                'detalhesApreensao', json_build_object(
                    'idSngb', COALESCE(apreensao.id, null),
                    'dataHoraInclusao', COALESCE(apreensao.ctr_dth_inc, null),
                    'dataHoraAtualizacao', COALESCE(apreensao.ctr_dth_atu, null),
                    'tipo', json_build_object(
                        'idSngb', COALESCE(tipo_apreensao.id, null),
                        'nome', COALESCE(tipo_apreensao.nome_tipo, null)
                    ),
                    'situacao', json_build_object(
                        'idSngb', COALESCE(situacao.id, null),
                        'nome', COALESCE(situacao.nome_situacao, null)
                    ),
                    'autoApreensao', json_build_object(
                        'emitido', COALESCE(apreensao.auto_apreensao_emitido, null),
                        'comprovanteEmitido', COALESCE(apreensao.comprovante_apreensao_emitido, null),
                        'responsavel', json_build_object(
                            'nome', COALESCE(apreensao.responsavel, null),
                            'documentosPrincipais', json_build_array(
                                json_build_object(
                                    'numero', COALESCE(apreensao.cpf_responsavel, null),
                                    'tipo', COALESCE(upper(split_part('cpf_responsavel', '_', 1)), null)
                                )
                            )
                        ),
                        'testemunhas', json_build_array(
                            json_build_object(
                                'nome', COALESCE(apreensao.responsavel, null), --DUVIDA
                                'documentosPrincipais', json_build_array(
                                    json_build_object(
                                        'numero', COALESCE(apreensao.cpf_responsavel, null),
                                        'tipo', COALESCE(upper(split_part('cpf_responsavel', '_', 1)), null)
                                    )
                                )
                            )
                        )
                    ),
                    'idOrgaoJulgador', COALESCE(NULL, null), --DUVIDA
                    'excluido', false --DUVIDA
                )
            ),

            -- Processos vinculados
            'processosVinculados', COALESCE(
                json_agg(
                    json_build_object(
                        'idSngb', COALESCE(bem_processo_vinculado.id, null),
                        'numeroProcesso', COALESCE(bem_processo_vinculado.numero_processo, null),
                        'dataHoraInclusao', to_char(bem_processo_vinculado.ctr_dth_inc, 'YYYY-MM-DD HH24:MI:SS.MS'),
                        'dataHoraAtualizacao', to_char(bem_processo_vinculado.ctr_dth_atu, 'YYYY-MM-DD HH24:MI:SS.MS'),
                        'nivelSigilo', COALESCE(bem_processo_vinculado.nivel_sigilo, 0),
                        'status', COALESCE(bem_processo_vinculado.status, null),
                        'principal', COALESCE(bem_processo_vinculado.principal, null),
                        'orgaoJulgador', json_build_object(
                            'tribunal', json_build_object(
                                'segmento', 'JUSTICA_ESTADUAL', --DUVIDA
                                'sigla', 'TJPB',  --DUVIDA
                                'nome', 'Tribunal de Justiça do Estado da Paraíba' --DUVIDA
                            ),
                            'id', 5570,  --DUVIDA
                            'nome', '4ª VARA CRIMINAL DE JOÃO PESSOA', --DUVIDA
                            'jurisdicao', json_build_object(
                                'id', 1334, --DUVIDA
                                'nome', 'JOAO PESSOA' --DUVIDA
                            )
                        ),
                        'orgaoJulgadorColegiado', json_build_object(
                            'id', COALESCE(NULL, null), --DUVIDA
                            'nome', COALESCE(NULL, null) --DUVIDA
                        ),
                        'excluido', false --DUVIDA
                    )
                ) FILTER (WHERE bem_processo_vinculado.id IS NOT NULL),
                '[]'::json
            ),
            'excluido', false
        ) AS json_output
    FROM snba.bem AS bem
    LEFT JOIN snba.padrao_especificacao AS padrao_especificacao ON padrao_especificacao.id = bem.padrao_especificacao_id
    LEFT JOIN snba.situacao AS situacao ON situacao.id = bem.situacao_id
    LEFT JOIN snba.subclasse AS subclasse ON subclasse.id = bem.subclasse_id
    LEFT JOIN snba.classe AS classe ON classe.id = subclasse.classe_id
    LEFT JOIN snba.categoria AS categoria ON categoria.id = classe.categoria_id
    LEFT JOIN snba.unidade_medida AS unidade_medida ON unidade_medida.id = bem.unidade_medida_id
    LEFT JOIN snba.apreensao AS apreensao ON apreensao.id = bem.apreensao_id
    LEFT JOIN snba.tipo_apreensao AS tipo_apreensao ON tipo_apreensao.id = apreensao.tipo_apreensao_id
    LEFT JOIN snba.bem_processo_vinculado AS bem_processo_vinculado ON bem_processo_vinculado.bem_id = bem.id
    WHERE bem.id = 530792
    GROUP BY 
        bem.id, bem.ctr_dth_inc, bem.ctr_dth_atu, bem.data_restricao,
        padrao_especificacao.id, situacao.id, categoria.id, classe.id, subclasse.id,
        unidade_medida.id, apreensao.id, tipo_apreensao.id;
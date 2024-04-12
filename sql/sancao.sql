WITH
  ceis AS (
    SELECT
      CASE
        WHEN sancionado_tipo = 'J' THEN UNACCENT(UPPER(TRIM(sancionado_razao_social_receita_federal)))
        ELSE UNACCENT(UPPER(TRIM(sancionado_nome_informado)))
      END AS nome,
      sancionado_documento AS documento,
      processo,
      'Inidônea/Suspensa' AS tipo,
      categoria AS detalhe_tipo,
      data_inicio,
      data_fim,
      orgao,
      fundamentacao_legal AS fundamentacao,
      NULL::numeric AS multa
    FROM sancao_ceis
  )
  , cepim AS (
    SELECT
      UNACCENT(UPPER(TRIM(sancionado))) AS nome,
      sancionado_cnpj AS documento,
      convenio_numero AS processo,
      'Impedida de licitar' AS tipo,
      NULL::text AS detalhe_tipo,
      NULL::date AS data_inicio,
      NULL::date AS data_fim,
      orgao AS orgao,
      motivo AS fundamentacao,
      NULL::numeric AS multa
    FROM sancao_cepim
  )
  , cnep AS (
    SELECT
      CASE
        WHEN sancionado_tipo = 'J' THEN UNACCENT(UPPER(TRIM(sancionado_razao_social_receita_federal)))
        ELSE UNACCENT(UPPER(TRIM(sancionado_nome_informado)))
      END AS nome,
      sancionado_documento AS documento,
      processo,
      'Punida' AS tipo,
      categoria AS detalhe_tipo,
      data_inicio,
      data_fim,
      orgao AS orgao,
      fundamentacao_legal AS fundamentacao,
      valor_multa AS multa
    FROM sancao_cnep
  )
  , acordo_leniencia AS (
    SELECT
      UNACCENT(UPPER(TRIM(sancionado_razao_social))) AS nome,
      sancionado_cnpj AS documento,
      processo,
      'Acordo de leniência' AS tipo,
      situacao AS detalhe_tipo,
      data_inicio,
      data_fim,
      orgao AS orgao,
      NULL::text AS fundamentacao,
      NULL::numeric AS multa
    FROM sancao_acordo_leniencia
    WHERE sancionado_cnpj ~ '^[0-9]+$'
  )
  , expulsao_adm_federal AS (
    SELECT
      UNACCENT(UPPER(TRIM(sancionado))) AS nome,
      sancionado_documento AS documento,
      processo,
      'Expulsão da Administração Federal' AS tipo,
      abrangencia AS detalhe_tipo,
      data_inicio,
      data_fim,
      orgao AS orgao,
      fundamentacao_legal AS fundamentacao,
      NULL::numeric AS multa
    FROM sancao_expulsao_adm_federal
  )
  , tudo AS (
    SELECT * FROM ceis
    UNION ALL
    SELECT * FROM cepim
    UNION ALL
    SELECT * FROM cnep
    UNION ALL
    SELECT * FROM acordo_leniencia
    UNION ALL
    SELECT * FROM expulsao_adm_federal
  )
SELECT
  CASE
    WHEN LENGTH(documento) = 11 THEN person_uuid(documento, nome)
    WHEN LENGTH(documento) = 14 THEN company_uuid(documento)
    ELSE uuid_nil()
  END AS object_uuid,
  *
FROM tudo

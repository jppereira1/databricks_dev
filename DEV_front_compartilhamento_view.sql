--CREATE OR REPLACE VIEW sandbox.originacao.front_compartilhamento_view AS

WITH
-- ============================ IMPORTS
related_work_items AS (
    SELECT * FROM raw_views.ocean_workspace_public.oce_related_work_items
),

raw_work_items AS (
    SELECT * FROM ocean.public_workspace.dim_work_items
),

dim_boards AS (
    SELECT * FROM ocean.public_workspace.dim_boards
),

dim_types AS (
    SELECT * FROM ocean.public_workspace.dim_work_item_types
),

dim_users AS (
    SELECT * FROM ocean.public_workspace.dim_users
),


typology_translate AS (
    SELECT * FROM sandbox.originacao.typology_code_translate_view
),

ocean_form_fields AS (
    SELECT * FROM sandbox.originacao.tab_ocean_forms_fields
),

dim_work_items AS (
    SELECT
    typology_translate.work_item_name,
    typology_translate.id_name_work_item,
    raw_work_items.* EXCEPT (work_item_name)

    FROM raw_work_items

    LEFT JOIN typology_translate ON raw_work_items.work_item_id = typology_translate.work_item_id
        AND raw_work_items.board_origin_id = typology_translate.board_origin_id
    
    WHERE lower(typology_translate.work_item_name) NOT LIKE '%teste%'
),

semifinal AS (
    SELECT

    -- ORIGEM
    src.origin_board_id AS origem_esteira_id,
    origin_board.board_name AS origem_esteira_nome,
    src.origin_work_item_id,
    origin_items.work_item_name AS origem_operacao_nome,
    src.origin_item_type_id AS origem_tipo_id,
    origin_types.work_item_type_name AS origem_tipo,
    origin_users.user_name AS origem_user_name,
    from_utc_timestamp(src.creation_timestamp, 'America/Sao_Paulo')::DATE AS origem_data_criacao, -- OK
    origem_vertical.field_value_final AS origem_vertical,

    -- TARGET
    src.target_board_id AS target_esteira_id,
    target_board.board_name AS target_esteira_nome,
    src.target_work_item_id,
    target_items.work_item_name AS target_operacao_nome,
    src.target_item_type_id AS target_tipo_id,
    target_types.work_item_type_name AS target_tipo,
    src.reply_user_id AS target_user_id_resposta,
    reply_users.user_name AS target_user_name_resposta,
    from_utc_timestamp(COALESCE(src.reply_timestamp,now()), 'America/Sao_Paulo')::DATE AS target_data_resposta,
    src.refuse_notes AS target_motivo_recusa,
    target_vertical.field_value_final AS target_vertical,

    CASE
      WHEN src.situation_description = 'AWA' THEN 'Pendding'
      WHEN src.situation_description = 'REF' THEN 'Refused'
      WHEN src.situation_description = 'ACC' AND target_items.is_deleted IS TRUE THEN 'Refused'
      WHEN src.situation_description = 'ACC' AND target_items.is_deleted IS FALSE THEN 'Accepted'
    END AS situation,
      -- AWA (Await) or (Pendding)
      -- REF (Refused)
      -- ACC (Accepted)
    CASE
        WHEN src.reply_timestamp::DATE = src.creation_timestamp::DATE THEN 1
        ELSE date_diff(COALESCE(src.reply_timestamp, NOW()), src.creation_timestamp)::INT
    END AS duracao_dias,
    from_utc_timestamp(src.record_timestamp, 'America/Sao_Paulo')::DATE AS data_ultima_alteracao,
    responsible_user.user_name AS responsavel_user_name,
    origin_items.is_deleted AS origin_deleted,
    target_items.is_deleted AS target_deleted,
    CASE
        WHEN target_estruturador.field_value_final = 'Guilherme Dias' THEN 'Guilherme Dias Costa Pinto'
        ELSE target_estruturador.field_value_final
    END AS target_estruturador,
    CASE
      WHEN qtd_empreendimentos.field_value_final :: INT < 0 THEN qtd_empreendimentos.field_value_final :: INT *(-1)
      WHEN qtd_empreendimentos.field_value_final :: INT = 0 THEN 1
      ELSE COALESCE(qtd_empreendimentos.field_value_final :: INT, 1)
    END AS qtd_empreendimentos,
    target_analista.field_value_final AS target_analista

    -- ================================ CAMPOS NÃO UTILIZADOS, PORÉM, DISPONÍVEIS
    -- src.accepted_subitems,
    -- src.work_item_share_id,
    -- src.accepted_files,
    -- src.accepted_forms,
    -- src.accepted_form_data,
    -- src.accepted_files_sub_items,
    -- src.creation_user_id,
    -- src.record_user_id AS record_user_id,
    -- record_users.user_name AS record_user_name,
    -- src.relation_type AS tipo_relacao,
    -- src.user_id_specific_sharing AS origem_user_id,
    -- src._is_deleted
    -- src._synced_at
    
    FROM related_work_items AS src
        -- TYPES
        LEFT JOIN dim_types AS origin_types ON src.origin_item_type_id = origin_types.work_item_type_id
        LEFT JOIN dim_types AS target_types ON src.target_item_type_id = target_types.work_item_type_id

        -- BOARDS
        LEFT JOIN dim_boards AS origin_board ON src.origin_board_id = origin_board.board_id
        LEFT JOIN dim_boards AS target_board ON src.target_board_id = target_board.board_id

        -- WORK ITEMS
        LEFT JOIN dim_work_items AS origin_items ON src.origin_work_item_id = origin_items.work_item_id
        LEFT JOIN dim_work_items AS target_items ON src.target_work_item_id = target_items.work_item_id

        -- USERS
        LEFT JOIN dim_users AS reply_users ON src.reply_user_id = reply_users.user_id
        LEFT JOIN dim_users AS origin_users ON src.creation_user_id = origin_users.user_id
        LEFT JOIN dim_users AS record_users ON src.record_user_id = record_users.user_id
        LEFT JOIN dim_users AS responsible_user ON src.user_id_specific_sharing = responsible_user.user_id

        -- LIDERES
        LEFT JOIN ocean_form_fields AS target_estruturador ON src.target_work_item_id = target_estruturador.work_item_id
            AND src.target_board_id = target_estruturador.board_id
            AND target_estruturador.field_name IN ('shkLiderVertical', 'shkEstruturadorDeNegocio')

        -- ANALISTAS
        LEFT JOIN ocean_form_fields AS target_analista ON src.target_work_item_id = target_analista.work_item_id
            AND src.target_board_id = target_analista.board_id
            AND target_analista.field_name IN ('shkAnalistaVertical', 'shkAnalistaDeCredito')
            
        LEFT JOIN ocean_form_fields AS qtd_empreendimentos ON src.target_work_item_id = qtd_empreendimentos.work_item_id
            AND src.target_board_id = qtd_empreendimentos.board_id
            AND qtd_empreendimentos.field_name = 'numQuantidadeDeEmpreendimentos' -- quantidade de empreendimentos por operação

        LEFT JOIN ocean_form_fields AS origem_vertical ON src.origin_work_item_id = origem_vertical.work_item_id
            AND src.origin_board_id = origem_vertical.board_id
            AND origem_vertical.field_name = 'losVertical'
        
        LEFT JOIN ocean_form_fields AS target_vertical ON src.target_work_item_id = target_vertical.work_item_id
            AND src.target_board_id = target_vertical.board_id
            AND target_vertical.field_name IN ('losVertical', 'losVErtical')


    WHERE src.origin_board_id = 331 -- SOMENTE FRONT
      AND src.target_board_id IN (153, 332) -- TARGET EQUITY e CRÉDITO
      AND origin_items.is_deleted IS FALSE -- REMOVE OS DELETADOS NA ORIGEM
      AND src.target_board_id NOT IN (313)
    ORDER BY src.record_timestamp DESC
),

final AS (
    SELECT
    origin_work_item_id,
    origem_esteira_nome,
    origem_operacao_nome,
    origem_tipo,
    origem_user_name,
    origem_data_criacao,
    target_esteira_nome,
    target_work_item_id,
    target_operacao_nome,
    target_tipo,
    target_user_name_resposta,
    target_data_resposta,
    situation,
    origem_vertical,
    target_vertical,
    duracao_dias,
    target_estruturador,
    target_motivo_recusa,
    target_estruturador AS estruturador,

    CASE
        WHEN target_esteira_id = 153 THEN 'Crédito'
        ELSE target_vertical
    END AS vertical_aprovada,

   qtd_empreendimentos,
   target_analista

    FROM semifinal

    WHERE year(target_data_resposta) >= 2022
)

SELECT
*
FROM
final
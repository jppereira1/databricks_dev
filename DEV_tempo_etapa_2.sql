-- CREATE OR REPLACE VIEW sandbox.originacao.tempo_etapa_2 AS

WITH

orig AS (
    SELECT * FROM sandbox.originacao.base_etapas_funil_view
    WHERE start_board_id IN (153, 332)
),

etapas AS (
    SELECT DISTINCT ordem_etapa, column_name
    FROM orig
    WHERE ordem_etapa NOT IN (9,10)
    ORDER BY ordem_etapa  ASC
),

cross_orig_etapas AS (
    SELECT DISTINCT work_item_id, etapas.ordem_etapa
    FROM orig
    CROSS JOIN etapas
),

join_cross_orig AS (
    SELECT DISTINCT
    coe.work_item_id,
    coe.ordem_etapa,
    orig_2.work_item_name,
    orig_2.start_board_id,
    orig_2.start_board_name,
    etapas.column_name,
    orig_1.data_inicio,
    orig_1.data_fim,
    orig_1.data_fim_OLD,
    orig_1.perdido_em,
    orig_1.is_lost,
    orig_1.duracao_dias

    FROM cross_orig_etapas coe
    LEFT JOIN orig AS orig_1 ON orig_1.work_item_id = coe.work_item_id AND orig_1.ordem_etapa = coe.ordem_etapa
    LEFT JOIN orig AS orig_2 ON orig_2.work_item_id = coe.work_item_id
    LEFT JOIN etapas ON etapas.ordem_etapa = coe.ordem_etapa

),

virtual_dates AS (

    SELECT
    jco.work_item_id,
    jco.ordem_etapa,
    jco.work_item_name,
    jco.start_board_id,
    jco.start_board_name,
    jco.column_name,
    jco.data_inicio,
    jco.data_fim,
    jco.data_fim_OLD,

    from_utc_timestamp(COALESCE(
            jco.data_inicio,
            LEAD(jco.data_inicio, 1) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.data_inicio, 2) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.data_inicio, 3) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.data_inicio, 4) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.data_inicio, 5) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.data_inicio, 6) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.data_inicio, 7) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa)
        ),'America/Sao_Paulo')::DATE AS data_inicio_virtual,

    from_utc_timestamp(COALESCE(
            jco.data_fim,
            LEAD(jco.data_inicio, 1) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.data_inicio, 2) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.data_inicio, 3) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.data_inicio, 4) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.data_inicio, 5) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.data_inicio, 6) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.data_inicio, 7) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa)
        ),'America/Sao_Paulo')::DATE AS data_fim_virtual,

    jco.perdido_em,
    COALESCE(
            jco.is_lost,
            LEAD(jco.is_lost, 1) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.is_lost, 2) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.is_lost, 3) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.is_lost, 4) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.is_lost, 5) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.is_lost, 6) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa),
            LEAD(jco.is_lost, 7) OVER (PARTITION BY jco.work_item_id ORDER BY jco.ordem_etapa)
    ) AS is_lost,
    jco.duracao_dias

    FROM join_cross_orig jco
),

perdidas_prorrogadas AS (
    SELECT
    orig.work_item_id,
    orig.ordem_etapa,
    orig.work_item_name,
    orig.start_board_id,
    orig.start_board_name,
    orig.column_name,
    orig.data_inicio,
    orig.data_fim,
    orig.data_fim_OLD,
    NULL AS data_inicio_virtual,
    NULL AS data_fim_virtual,
    orig.perdido_em,
    orig.is_lost,
    orig.duracao_dias

    FROM orig
    WHERE ordem_etapa >8
),

union_all AS (
    SELECT * FROM virtual_dates
    WHERE virtual_dates.data_inicio_virtual IS NOT NULL
    UNION ALL
    SELECT * FROM perdidas_prorrogadas
),

final AS (
    SELECT *,
    datediff(data_fim_virtual, data_inicio_virtual) as duracao_dias_virtual  
     FROM union_all
)

SELECT * FROM final
-- WHERE   work_item_id = 180451
order by data_inicio desc
 
-- SELECT * FROM virtual_dates WHERE work_item_id = '180451'
-- SELECT * FROM join_cross_orig WHERE work_item_id = '11679'
-- SELECT * FROM cross_orig_etapas WHERE work_item_id = '11679'

-- SELECT * FROM final WHERE work_item_id = 180451
-- SELECT * FROM etapas
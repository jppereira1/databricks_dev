CREATE OR REPLACE TABLE sandbox.originacao.tab_front_compartilhamento AS

WITH 

all_operations AS (
    SELECT * FROM sandbox.originacao.all_operations_info_table
),

sub_final AS (
   SELECT
    origin_work_item_id,
    split(origem_operacao_nome, ",")[0] AS codigo_ocean,
    from_utc_timestamp(origem_data_criacao, 'America/Sao_Paulo')::DATE AS data_criacao,
    target_esteira_nome,
    target_work_item_id,
    target_operacao_nome,
    from_utc_timestamp(target_data_resposta, 'America/Sao_Paulo')::DATE AS data_resposta, 
    situation,
    origem_vertical,
    duracao_dias,
    target_motivo_recusa,
    qtd_empreendimentos,
    CASE WHEN estruturador = 'Guilherme Dias' THEN 'Guilherme Dias Costa Pinto' ELSE estruturador END AS estruturador,
    CASE WHEN target_esteira_nome = 'Crédito' THEN 'Crédito' ELSE target_vertical END AS vertical_aprovada,
    CASE WHEN target_esteira_nome = 'Crédito' THEN 1 ELSE qtd_empreendimentos END AS qtd_ajustada,
    target_analista

    FROM sandbox.originacao.front_compartilhamento_view

    WHERE target_esteira_nome IN ('Crédito', 'Equity') AND
        origem_operacao_nome NOT IN ('teste','TESTE', 'Teste')
    ORDER BY data_resposta DESC

),

final AS (
    SELECT
        sf.origin_work_item_id,
        sf.codigo_ocean,
        sf.data_criacao,
        sf.target_esteira_nome,
        sf.target_work_item_id,
        sf.target_operacao_nome,
        sf.data_resposta,
        sf.situation,
        sf.origem_vertical,
        sf.duracao_dias,
        sf.target_motivo_recusa,
        sf.qtd_empreendimentos,
        sf.estruturador,
        sf.vertical_aprovada,
        sf.qtd_ajustada,
        sf.target_analista,
        ao.etapa_origem,
        ao.alocacao_total,

        -- REGRA QUE DEIXA NULO A QUANTIDADE DE EMPREENDIMENTOS PARA AS OPERAÇÕES REPETIDAS
        CASE 
            WHEN COUNT(sf.origin_work_item_id) OVER (PARTITION BY sf.origin_work_item_id) > 1 AND sf.situation = "Refused" THEN NULL
            ELSE sf.qtd_ajustada
        END AS qtd_empreendimentos_2

    FROM sub_final sf

    LEFT JOIN all_operations AS ao ON ao.operacao_id = sf.codigo_ocean
    ORDER BY sf.data_resposta DESC
)

SELECT * FROM final
LIMIT 10;
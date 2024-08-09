-- CREATE OR REPLACE VIEW sandbox.originacao.motivo_perda_ib_view AS

WITH forms_ocean AS (
    SELECT * 
    FROM sandbox.originacao.ocean_forms_fields_view
    WHERE field_id IN (5536, 5537, 5538, 5542, 5543, 5544, 71, 144) 
      AND board_id IN (332, 153)
    -- WHERE board_id IN (332, 153)
    ORDER BY label_name ASC
),

all_operations AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY ao.work_item_id ORDER BY ao.fee_auditoria_juridica_total) AS rn
    FROM sandbox.originacao.all_operations_info_view ao
),

final AS (
    SELECT 
        ao.work_item_id,
        fo.id_name_work_item,
        ao.operacao_nome AS work_item_name,
        fo.board_id,
        ao.esteira_origem AS board_name,
        fo.field_id,
        fo.field_type,
        fo.field_type_reference,
        fo.label_name,
        fo.field_value_origin,
        fo.field_value_final AS motivo_da_perda,
        consideracao_da_perda.field_value_final AS consideracao_da_perda,
        fo.is_null,
        ao.data_inicio as data_inicio,
        ao.data_fim,
        ao.coluna_antes_da_perda
    FROM all_operations ao 

    LEFT JOIN forms_ocean fo ON ao.work_item_id = fo.work_item_id
        AND fo.field_id IN (5536,5537,5538,5542, 5543, 5544)

    LEFT JOIN forms_ocean AS consideracao_da_perda ON consideracao_da_perda.work_item_id = ao.work_item_id
        AND consideracao_da_perda.field_name = 'utxConsideracaoDaPerda'

    WHERE ao.data_fim >= '2024-01-01' 
      AND ao.work_item_id NOT IN (209081, 208987, 209045, 207825,180361,208008)
      AND ao.rn = 1
      AND ao.esteira_origem IN ('Equity', 'Crédito')
      AND ao.etapa_origem = 'Perdida'
)

SELECT * 
FROM final
-- WHERE work_item_id = 194839
ORDER BY data_inicio, work_item_id DESC;

-- SELECT * FROM all_operations
-- limit 5

-- SELECT * FROM forms_ocean
-- WHERE work_item_id = 175609





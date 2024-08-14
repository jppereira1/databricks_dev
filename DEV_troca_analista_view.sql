CREATE OR REPLACE VIEW sandbox.originacao.troca_analista_view AS

WITH exploded_json AS (
  SELECT
    log.form_log_data_id,
    wit.work_item_id,
    wit.form_id,
    boa.board_id,
    log.log_type,
    -- os objetos em jason vem da tabela raw_views.ocean_forms_dbo.oce_forms_logs_data
    from_json(get_json_object(previous_json_description, '$.notDuplicableGroups.fields'), 'MAP<STRING,STRING>') AS previous_json_map,
    from_json(get_json_object(new_json_description, '$.notDuplicableGroups.fields'), 'MAP<STRING,STRING>') AS new_json_map,
    log.creation_timestamp,
    log.creation_user_id
  FROM raw_views.ocean_forms_dbo.oce_forms_work_items wit
 
  INNER JOIN raw_views.ocean_forms_dbo.oce_forms_boards boa
  ON wit.form_id = boa.form_id
 
  INNER JOIN raw_views.ocean_forms_dbo.oce_forms_logs_data log
  ON wit.work_item_id = log.work_item_id
  AND wit.form_id = log.form_id
 
  WHERE boa.board_id IN (332, 153) 
  AND log.log_type = 'UPD'
),

final AS (
    SELECT
    log.work_item_id,
    wit.name AS work_item_name,
    log.board_id,
    COALESCE(log.previous_json_map['shkAnalistaDeCredito'], log.previous_json_map['shkAnalistaVertical']) AS previous_user_id,
    us2.name AS previous_user_name,
    COALESCE(log.new_json_map['shkAnalistaDeCredito'], log.new_json_map['shkAnalistaVertical']) AS new_user_id,
    us3.name AS new_user_name,  
    date_format(log.creation_timestamp,'dd-MM-yyyy') AS data_mudanca,
    log.creation_user_id,
    us1.name AS creation_user_name,
    us1.email
    FROM exploded_json log
    
    INNER JOIN raw_views.ocean_workspace_public.users us1
    ON log.creation_user_id = us1.id
    
    INNER JOIN raw_views.ocean_workspace_public.work_items wit
    ON wit.id = log.work_item_id
    
    LEFT JOIN raw_views.ocean_workspace_public.users us2
    ON COALESCE(log.previous_json_map['shkAnalistaDeCredito'], log.previous_json_map['shkAnalistaVertical']) = us2.id
    
    LEFT JOIN raw_views.ocean_workspace_public.users us3
    ON COALESCE(log.new_json_map['shkAnalistaDeCredito'], log.new_json_map['shkAnalistaVertical']) = us3.id
    
    WHERE COALESCE(log.previous_json_map['shkAnalistaDeCredito'], log.previous_json_map['shkAnalistaVertical']) 
        <> COALESCE(log.new_json_map['shkAnalistaDeCredito'], log.new_json_map['shkAnalistaVertical'])
    
    ORDER BY log.creation_timestamp DESC)

SELECT * FROM final
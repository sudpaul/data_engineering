# Databricks notebook source
# MAGIC %md
# MAGIC ##### Gold - Generic Load Jinja2 template

# COMMAND ----------

def gld_generic_start():
  return r"""
-- *----------------------------------------------*
-- STEP 1: Create Staging table
-- *----------------------------------------------*
CREATE OR REPLACE TABLE {{template_params['work_database']}}.{{template_params['worker_table']}}_stg 
	USING DELTA
	AS 
	SELECT
        secure_hash(array(
        {%- for col, tag in template_params['pk_hash_cols'].items() -%}
          {%- if loop.index > 1 -%},{%- endif -%}
            COALESCE(cast(`{{col}}` as STRING),"")
          {% endfor %}),'|') AS `Pk_Hash`
        ,secure_hash(array(
		{%- for col, tag in template_params['row_hash_cols'].items() -%}
          {%- if loop.index > 1 -%},{%- endif -%}
            COALESCE(CAST(`{{col}}` as STRING),"")
          {% endfor %}),'|') AS `Row_Hash`
        {##}{%- if template_params['hist_stitch_on'] -%}
		,CAST(`{{template_params['hist_stitch_col']}}` as TIMESTAMP)  AS `Effective_Dttm`
		{%- else -%}
		,`Effective_Dttm` AS `Effective_Dttm` 
		{%- endif -%}{##}
        ,CAST('9999-12-31 00:00:00' AS TIMESTAMP) AS `Expiry_Dttm`
        ,'{{schema_dict['source_application_name']}}' AS `Source_App_Name` 
        ,upper(substring(`Record_Type`,0,1)) AS `Record_Type`
        ,CAST('{{template_params['process_start_time_stamp']}}' AS TIMESTAMP) AS `Record_Insert_Dttm`
        ,CAST(NULL AS TIMESTAMP) AS `Record_Update_Dttm` 
        ,'{{template_params['pipeline_run_id']}}' AS `Process_Instance_Id` 
        ,CAST(NULL as STRING) AS `Update_Process_Instance_Id` 
        ,CAST(1 AS BOOLEAN) AS `Is_Current`
        {##}{%- for col in schema_dict['target_columns'] -%}
        {%- if  not(col['ignore']) or col['is_primary_key_col'] -%}
        {%- if  col['adb_encryption_type'] == 'DET' -%}
        ,encrypt_scala_det_binary(CAST(`{{col['column_name']}}` AS STRING)) AS `{{col['column_name']}}`
       
        {##}
        {%- elif  col['adb_encryption_type'] == 'NDET' -%}
        ,encrypt_scala_ndet_binary(CAST({%- if not(col['nullable']) -%}COALESCE({%- endif -%}`{{col['column_name']}}` {%- if not(col['nullable']) -%},CAST('{{col['default_value']}}' as {{col['adb_data_type']}})){%- endif -%} AS STRING)) AS `{{col['column_name']}}`
        {##}
        {%- else -%}
        ,{%- if not(col['nullable']) -%}COALESCE({%- endif -%}`{{col['column_name']}}` {%- if not(col['nullable']) -%},CAST('{{col['default_value']}}' as {{col['adb_data_type']}})){%- endif -%} AS `{{col['column_name']}}`
        {% endif %}
        {%- endif -%}
		{%- endfor -%}
        {% for ik_name in template_params['integration_key_cols_per_name'] %}
        ,secure_hash(array(
          {%- for col in template_params['integration_key_cols_per_name'][ik_name] -%}
          {%- if loop.index > 1 -%},{%- endif -%}
            COALESCE(cast(`{{col}}` as STRING),""){##}
          {% endfor %} ),'|') AS `{{ik_name}}`
          {%- endfor -%}{##}
        ,CAST(date_format(timestamp '{{template_params['process_start_time_stamp']}}','yyyyMM') as INT) as `Year_Month`
	FROM {{template_params['work_database']}}.{{template_params['worker_table']}}_mrg;;
	
    
ANALYZE TABLE {{template_params['work_database']}}.{{template_params['worker_table']}}_stg COMPUTE STATISTICS;;

-- *----------------------------------------------*
-- STEP 2: Create/Load existing gold table if it does not exist
-- *----------------------------------------------*
CREATE TABLE IF NOT EXISTS {{template_params['main_database']}}.{{schema_dict['object_name']}}
	(
    `Pk_Hash` BINARY
    ,`Row_Hash` BINARY
    ,`Effective_Dttm` TIMESTAMP
    ,`Expiry_Dttm` TIMESTAMP
    ,`Source_App_Name` STRING
    ,`Record_Type` STRING
    ,`Record_Insert_Dttm` TIMESTAMP
    ,`Record_Update_Dttm` TIMESTAMP
    ,`Process_Instance_Id` STRING
    ,`Update_Process_Instance_Id` STRING
    ,`Is_Current` BOOLEAN
    {##}{%- for col in schema_dict['target_columns'] -%}
    {%- if  not(col['ignore']) or col['is_primary_key_col'] -%}
    {%- if  col['adb_encryption_type'] in ('DET','NDET') -%}
    ,`{{col['column_name']}}` BINARY
    {##}
    {%- else -%}
    ,`{{col['column_name']}}` {{col['adb_data_type']}} {% if not(col['nullable']) %} NOT NULL{% endif %}
    {% endif %}
    {%- endif -%}
    {%- endfor -%}
    {%- for ik_name in template_params['integration_key_cols_per_name'] -%}
    ,`{{ik_name}}` BINARY
    {% endfor %}
    ,`Year_Month` INT
)
USING DELTA
PARTITIONED BY (`Year_Month`)
LOCATION '{{template_params['outgoing_path']}}';;

ANALYZE TABLE {{template_params['main_database']}}.{{schema_dict['object_name']}} COMPUTE STATISTICS

-- Now execute load specific template;;
"""

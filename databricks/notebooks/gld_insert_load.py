# Databricks notebook source
# MAGIC %md
# MAGIC ##### Gold - Insert Load Jinja2 template

# COMMAND ----------

def gld_insert():
    return r"""
-- *----------------------------------------------*
-- STEP 3.1: Identify worker records to insert and Gold records to update
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['work_database']}}.{{template_params['worker_table']}}_load;;
CREATE TABLE {{template_params['work_database']}}.{{template_params['worker_table']}}_load
    AS
    SELECT
        `pk_hash` AS `pk_hash`
        ,`row_hash` AS `row_hash`
		,`effective_dttm` AS `effective_dttm`
        ,`expiry_dttm` AS `expiry_dttm`
        ,`source_app_name` AS `source_app_name`
        ,"I" AS `record_type`
        ,`record_insert_dttm` AS `record_insert_dttm`
		,`record_update_dttm` AS `record_update_dttm`
		,`process_instance_id` AS `process_instance_id`
		,`update_process_instance_id` AS `update_process_instance_id`
        ,`is_current` AS `is_current`
        {##}{%- for col in schema_dict['target_columns'] -%}
        {%- if  not(col['ignore']) or col['is_primary_key_col'] -%}
        {%- if  col['adb_encryption_type'] in ('DET','NDET') -%}
        ,`{{col['column_name']}}` BINARY 
        ,`{{col['column_name']}}` as `{{col['column_name']}}`
        {% endif %}
        {%- endfor -%}
        {%- for ik_name in template_params['integration_key_cols_per_name'] -%}
        ,`{{ik_name}}` BINARY
        {% endfor %}
        ,`Year_Month`
    FROM {{template_params['work_database']}}.{{template_params['worker_table']}}_stg;;

ANALYZE TABLE {{template_params['work_database']}}.{{template_params['worker_table']}}_load COMPUTE STATISTICS;;
    
-- *----------------------------------------------*
-- STEP 3.2: Identify worker records which matches PK_Hash and Row_Hash
-- *----------------------------------------------*
DELETE FROM {{template_params['work_database']}}.{{template_params['worker_table']}}_load load
WHERE EXISTS (
    SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['object_name']}} gld
    WHERE load.pk_hash = gld.pk_hash AND load.row_hash = gld.row_hash
);;
"""
